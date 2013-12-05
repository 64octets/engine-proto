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
    /**
     * Returns an iterator that merge other iterators.
     */
    public static AtomIterator merge(List<AtomIterator> iterators, int now)
    {
        assert !iterators.isEmpty();
        return iterators.size() == 1 ? iterators.get(0) : new AtomMergeIterator(iterators, now);
    }

    /**
     * Merge 2 iterators using the provided MergeWriter.
     */
    public static AtomIterator merge(AtomIterator left, AtomIterator right, MergeWriter writer, int now)
    {
        return new AtomMergeIterator(Arrays.asList(left, right), writer, now);
    }

    /**
     * Returns an iterator that filter any tombstone older than {@code gcBefore}
     * in {@code iterator}.
     */
    public static AtomIterator purge(final AtomIterator iterator, final int now, final int gcBefore)
    {
        return new AbstractFilteringAtomIterator(iterator)
        {
            private final ReusableRow result = new ReusableRow(iterator.metadata(), iterator.metadata().regularColumns().length);

            protected RangeTombstone filter(RangeTombstone rt)
            {
                return rt.delTime().localDeletionTime() < gcBefore ? null : rt;
            }

            protected Row filter(Row row)
            {
                Rows.Writer writer = result.writer();
                boolean writtenOne = false;
                for (Cell cell : row)
                {
                    if (cell.isDeleted(now))
                        continue;

                    writtenOne = true;
                    writer.addCell(cell);
                }
                return writtenOne ? result : null;
            }
        };
    }

    /**
     * Returns an iterator that only return atoms from {@code iterator} until
     * {@code counter} has counted enough data.
     */
    public static AtomIterator count(final AtomIterator iterator, final DataCounter counter, final int now)
    {
        counter.countPartition();
        return new AbstractFilteringAtomIterator(iterator)
        {
            private final ReusableRow result = new ReusableRow(iterator.metadata(), iterator.metadata().regularColumns().length);

            @Override
            protected boolean isDone()
            {
                return counter.hasEnoughDataForPartition();
            }

            protected RangeTombstone filter(RangeTombstone rt)
            {
                return rt;
            }

            protected Row filter(Row row)
            {
                int live = 0;
                int tombstoned = 0;
                for (Cell cell : row)
                {
                    if (cell.isDeleted(now))
                        ++tombstoned;
                    else
                        ++live;
                }
                counter.countRow(live, tombstoned);
                return row;
            }
        };
    }

    /**
     * Returns an iterator that clone all ByteBuffer using the provided allocator.
     */
    public static AtomIterator cloningIterator(final AtomIterator iterator, final Allocator allocator)
    {
        return new AbstractFilteringAtomIterator(iterator)
        {
            private final CloningRangeTombstone rangeTombstone = new CloningRangeTombstone(iterator.metadata(), allocator);
            private final ReusableRow result = new ReusableRow(iterator.metadata(), iterator.metadata().regularColumns().length);

            protected RangeTombstone filter(RangeTombstone rt)
            {
                rangeTombstone.set(rt.min(), rt.max(), rt.delTime());
                return rangeTombstone;
            }

            protected Row filter(Row row)
            {
                Rows.Writer writer = result.writer();
                for (Cell cell : row)
                    writer.addCell(cell.column(), cell.isTombstone(), copy(cell.key()), copy(cell.value()), cell.timestamp(), cell.ttl(), cell.localDeletionTime());
                return result;
            }

            private ByteBuffer copy(ByteBuffer bb)
            {
                return bb == null ? null : allocator.clone(bb);
            }
        };
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
        private final MergeWriter writer;
        private final Rows.MergeHelper helper;

        private AtomMergeIterator(List<AtomIterator> iterators, MergeWriter writer, int now)
        {
            assert iterators.size() > 1;
            // TODO: we could assert all iterators are on the same CF && key
            this.metadata = iterators.get(0).metadata();
            this.partitionKey = iterators.get(0).getPartitionKey();
            this.partitionLevelDeletion = collectPartitionLevelDeletion(iterators);
            this.mergeIterator = MergeIterator.get(iterators, metadata.comparator().atomComparator(), createReducer(iterators.size()));
            this.writer = writer;
            this.helper = new Rows.MergeHelper(writer, now, iterators.size());

            writer.setPartitionLevelDeletion(partitionLevelDeletion);
        }

        private AtomMergeIterator(List<AtomIterator> iterators, int now)
        {
            this(iterators, new MergeWriter(iterators.get(0).metadata()), now);
        }

        private MergeIterator.Reducer<Atom, Atom> createReducer(final int size)
        {
            return new MergeIterator.Reducer<Atom, Atom>()
            {
                private Atom.Kind nextKind;
                private RangeTombstone rangeTombstone;
                private final List<Row> rows = new ArrayList<>(size);

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
                            writer.tracker.update(writer.row);
                            // Because shadowed cells are skipped, the row could be empty. In which case
                            // we return and the enclosing iterator will just skip it.
                            return writer.row.cellsCount() == 0 ? null : writer.row;
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
                        writer.row.clear();
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
    }

    /**
     * Specific row writer for merge operations that rewrite the same reusable row every time.
     * <p>
     * This writer also skips cells shadowed by range tombstones when writing.
     */
    public static class MergeWriter implements Rows.Writer
    {
        protected final ReusableRow row;
        protected Rows.Writer writer;
        protected final TombstoneTracker tracker;
        protected DeletionTime topLevelDeletion;

        public MergeWriter(Layout metadata)
        {
            // TODO: we could maybe do better for the estimation of the initial capacity of that reusable row. For instance,
            // the AtomIterator interface could return an estimate of the size of rows it will return, and we could average/max
            // it over all iterators.
            this.row = new ReusableRow(metadata, metadata.regularColumns().length);
            this.writer = row.writer();
            this.tracker = new TombstoneTracker(metadata.comparator());
        }

        protected void onSkippedCell() {}
        protected void onAddedCell() {}
        protected void onSkippedRow() {}

        // We set that inside AtomMergeIterator but the MergeWriter is created outside so we can set that in the ctor.
        // (we could have a some Builder interface instead but that is probably not worth the trouble).
        private void setPartitionLevelDeletion(DeletionTime delTime)
        {
            this.topLevelDeletion = delTime;
        }

        public void setClusteringColumn(int i, ByteBuffer value)
        {
            writer.setClusteringColumn(i, value);
        }

        public void addCell(Cell cell)
        {
            addCell(cell.column(), cell.isTombstone(), cell.key(), cell.value(), cell.timestamp(), cell.ttl(), cell.localDeletionTime());
        }

        public void addCell(Column c, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime)
        {
            if (topLevelDeletion.markedForDeleteAt() >= timestamp || tracker.isDeleted(row, timestamp))
            {
                onSkippedCell();
                return;
            }

            onAddedCell();
            writer.addCell(c, isTombstone, key, value, timestamp, ttl, deletionTime);
        }

        public void done()
        {
            writer.done();
            if (row.cellsCount() == 0)
                onSkippedRow();

            row.reset();
        }

        public void clear()
        {
            row.clear();
        }
    }
}
