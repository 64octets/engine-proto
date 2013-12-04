/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.engine;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;

public abstract class AtomIterators
{
    public static AtomIterator merge(List<AtomIterator> iterators, int now)
    {
        assert !iterators.isEmpty();
        return iterators.size() == 1 ? iterators.get(0) : new AtomMergeIterator(iterators, now);
    }

    /**
     * A wrapper over MergeIterator to implement the AtomIterator interface.
     *
     * NOTE: MergeIterator has to look-ahead in the merged iterators during hasNext, which means that
     * not only next() is destructive of the previously returned element but hasNext() is too. That
     * being said, it's not really harder to consider that both method are always destructive so we
     * live with it.
     */
    private static class AtomMergeIterator extends AbstractIterator<Atom> implements AtomIterator
    {
        private static final int DEFAULT_ROW_CAPACITY = 10;

        private final Layout metadata;
        private final DecoratedKey partitionKey;
        private final DeletionTime partitionLevelDeletion;
        private final MergeIterator<Atom, Atom> mergeIterator;
        private final int now;

        private AtomMergeIterator(List<AtomIterator> iterators, int now)
        {
            assert iterators.size() > 1;
            // TODO: we could assert all iterators are on the same CF && key
            this.metadata = iterators.get(0).metadata();
            this.partitionKey = iterators.get(0).getPartitionKey();
            this.partitionLevelDeletion = collectPartitionLevelDeletion(iterators);
            this.mergeIterator = MergeIterator.get(iterators, metadata.comparator().atomComparator(), createReducer(iterators.size()));
            this.now = now;
        }

        private MergeIterator.Reducer<Atom, Atom> createReducer(final int size)
        {
            return new MergeIterator.Reducer<Atom, Atom>()
            {
                private Atom.Kind nextKind;
                private RangeTombstone rangeTombstone;
                private final List<Row> rows = new ArrayList<>(size);
                // TODO: we could maybe do better for the estimation of the initial capacity of that reusable row. For instance,
                // the AtomIterator interface could return an estimate of the size of rows it will return, and we could average/max
                // it over all iterators.
                private final ReusableRow row = new ReusableRow(metadata, metadata.regularColumns().length);
                private final SkipShadowedWriter writer = new SkipShadowedWriter(row, new TombstoneTracker(metadata.comparator()));
                private final Rows.MergeHelper helper = new Rows.MergeHelper(writer, now, size);

                public boolean trivialReduceIsTrivial()
                {
                    return true;
                }

                public void reduce(Atom current)
                {
                    nextKind = current.kind();
                    switch (nextKind)
                    {
                        case ROW:
                            rows.add((Row)current);
                            break;
                        case RANGE_TOMBSTONE:
                            rangeTombstone = (RangeTombstone)current;
                            break;
                        case COLLECTION_TOMBSTONE:
                            // TODO
                            break;
                    }
                }

                protected Atom getReduced()
                {
                    switch (nextKind)
                    {
                        case ROW:
                            Rows.merge(metadata, rows, helper);
                            writer.tracker.update(row);
                            // Because shadowed cells are skipped, the row could be empty. In which case
                            // we return and the enclosing iterator will just skip it.
                            return row.cellsCount() == 0 ? null : row;
                        case RANGE_TOMBSTONE:
                            writer.tracker.update(rangeTombstone);
                            return rangeTombstone;
                        case COLLECTION_TOMBSTONE:
                            throw new UnsupportedOperationException(); // TODO
                    }
                    throw new AssertionError();
                }

                protected void onKeyChange()
                {
                    if (nextKind == Atom.Kind.ROW)
                    {
                        rows.clear();
                        writer.reset();
                    }
                }
            };
        }

        private static DeletionTime collectPartitionLevelDeletion(List<AtomIterator> iterators)
        {
            DeletionTime delTime = DeletionTime.LIVE;
            for (AtomIterator iter : iterators)
                if (delTime.compareTo(iter.partitionLevelDeletion()) < 0)
                    delTime = iter.partitionLevelDeletion();
            return delTime;
        }

        protected Atom computeNext()
        {
            while (mergeIterator.hasNext())
            {
                Atom atom = mergeIterator.next();
                if (atom != null)
                    return atom;
            }
            return endOfData();
        }

        public Layout metadata()
        {
            return metadata;
        }

        public DecoratedKey getPartitionKey()
        {
            return partitionKey;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public void close()
        {
            mergeIterator.close();
        }

        /**
         * Trivial wrapper over a reusable row that leaves out cells shadowed by
         * range tombstones when writing.
         */
        private static class SkipShadowedWriter implements Rows.Writer
        {
            private final ReusableRow row;
            private final AbstractRow.Writer writer;
            public final TombstoneTracker tracker;

            public SkipShadowedWriter(ReusableRow row, TombstoneTracker tracker)
            {
                this.row = row;
                this.writer = row.writer();
                this.tracker = tracker;
            }

            public void setClusteringColumn(int i, ByteBuffer value)
            {
                writer.setClusteringColumn(i, value);
            }

            public void addCell(Column c, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime)
            {
                if (!tracker.isDeleted(row, timestamp))
                    writer.addCell(c, isTombstone, key, value, timestamp, ttl, deletionTime);
            }

            public void done()
            {
                writer.done();
            }

            public void reset()
            {
                writer.reset();
            }
        }
    }
}
