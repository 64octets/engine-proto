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

import java.util.concurrent.atomic.AtomicReference;

// This is somewhat dumb, this will be pretty inefficient for large parttion. This is for testing only for now.
public class COWMutablePartition implements MutablePartition
{
    private final AtomicReference<Partition> ref;

    public COWMutablePartition(Layout metadata, DecoratedKey partitionKey)
    {
        this.ref = new AtomicReference<>(Partitions.empty(metadata, partitionKey));
    }

    public long apply(PartitionUpdate update, Allocator allocator, IndexUpdater indexer)
    {
        // TODO: we could add a step that bails early from the CAS loop if we're beaten (by stopping the iteration), but
        // again, this is dumb implem for tests.
        while (true)
        {
            Partition current = ref.get();
            AtomIterator updateIterator = AtomIterators.cloningIterator(update.atomIterator(), allocator);
            // TODO: means we use null for NullUpdater
            AtomIterators.MergeWriter writer = indexer == null
                                             ? new AtomIterators.MergeWriter(current.metadata())
                                             : new MergeWithIndexWriter(current.metadata(), indexer);
            AtomIterator merged = AtomIterators.merge(current.atomIterator(), updateIterator, writer, update.now());

            Partition updated = ArrayBackedPartition.accumulate(merged, current.rowCount() + update.rowCount(), current.cellCount() + update.cellCount());
            if (ref.compareAndSet(current, updated))
                return updated.cellCount() - current.cellCount(); // TODO: We need a better estimation of the changed size.
        }
    }

    public Layout metadata()
    {
        return ref.get().metadata();
    }

    public ClusteringComparator comparator()
    {
        return ref.get().comparator();
    }

    public DecoratedKey getPartitionKey()
    {
        return ref.get().getPartitionKey();
    }

    public DeletionInfo deletionInfo()
    {
        return ref.get().deletionInfo();
    }

    public boolean isEmpty()
    {
        return ref.get().isEmpty();
    }

    public int rowCount()
    {
        return ref.get().rowCount();
    }

    public int cellCount()
    {
        return ref.get().cellCount();
    }

    public Row findRow(RowPath path)
    {
        return ref.get().findRow(path);
    }

    public AtomIterator atomIterator()
    {
        return ref.get().atomIterator();
    }

    public AtomIterator atomIterator(Slices slices)
    {
        return ref.get().atomIterator(slices);
    }

    private static class MergeWithIndexWriter extends AtomIterators.MergeWriter implements Rows.Merger
    {
        private enum UpdateKind { NONE, INSERTED, UPDATED, REMOVED };

        private final IndexUpdater updater;
        private Resolution lastResolution;
        private UpdateKind update = UpdateKind.NONE;
        private Row left;
        private Row right;

        public MergeWithIndexWriter(Layout metadata, IndexUpdater updater)
        {
            super(metadata);
            this.updater = updater;
        }

        public void newMergedRows(Row left, Row right)
        {
            this.left = left;
            this.right = right;
        }

        public void nextCellResolution(Resolution resolution)
        {
            lastResolution = resolution;
        }

        @Override
        protected void onSkippedCell()
        {
            // We're skipping the cell because it's shadowed. Unless that cell was only on right,
            // this count as an update of the existing row.
            if (lastResolution != Rows.Merger.Resolution.ONLY_IN_RIGHT)
                update = UpdateKind.UPDATED;
        }

        @Override
        protected void onAddedCell()
        {
            // Left is the original row, right is the update. If we see no update from right,
            // then we haven't update anything. Otherwise, it's an update unless we've only
            // seen ONLY_IN_RIGHT resolutions (i.e. left had nothing, not even something updated)
            switch (lastResolution)
            {
                case ONLY_IN_LEFT:
                case MERGED_FROM_LEFT:
                    if (update == UpdateKind.INSERTED)
                        update = UpdateKind.UPDATED;
                    break;
                case ONLY_IN_RIGHT:
                    if (update == UpdateKind.NONE)
                        update = UpdateKind.INSERTED;
                    break;
                case MERGED_FROM_RIGHT:
                    update = UpdateKind.UPDATED;
                    break;
            }
        }

        @Override
        protected void onSkippedRow()
        {
            // The whole row ended up being skipped. This is a row removal unless at this
            // point the update was INSERTED (in which case in means the update isn't changing
            // anything after all)
            if (update != UpdateKind.INSERTED)
                update = UpdateKind.REMOVED;
        }

        @Override
        public void done()
        {
            super.done();
            switch (update)
            {
                case INSERTED:
                    updater.insert(right);
                    break;
                case UPDATED:
                    updater.update(left, right);
                    break;
                case REMOVED:
                    updater.remove(left);
                    break;
            }
            update = UpdateKind.NONE;
        }
    }
}
