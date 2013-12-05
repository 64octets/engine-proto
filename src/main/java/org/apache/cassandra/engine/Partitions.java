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

public abstract class Partitions
{
    private Partitions() {}

    // TODO: we can't have a unique empty partition object as we need at least the
    // metadata and partiton key. So maybe allocating an object to represent an
    // empty partition is too much and we should use null. But it's easier enough to
    // use an empty object that we'll try with it and assert later if it's worth
    // micro-optimizing this.
    public static Partition empty(Layout metadata, DecoratedKey partitionKey)
    {
        return new EmptyPartition(metadata, partitionKey);
    }

    private static class EmptyPartition implements Partition
    {
        private final Layout metadata;
        private final DecoratedKey partitionKey;

        private EmptyPartition(Layout metadata, DecoratedKey partitionKey)
        {
            this.metadata = metadata;
            this.partitionKey = partitionKey;
        }

        public Layout metadata()
        {
            return metadata;
        }

        public ClusteringComparator comparator()
        {
            return metadata.comparator();
        }

        public DecoratedKey getPartitionKey()
        {
            return partitionKey;
        }

        public DeletionInfo deletionInfo()
        {
            return DeletionInfo.live();
        }

        // No values (even deleted), live deletion infos
        public boolean isEmpty()
        {
            return true;
        }

        public int rowCount()
        {
            return 0;
        }

        public int cellCount()
        {
            return 0;
        }

        public Row findRow(RowPath path)
        {
            return null;
        }

        // Iterator over the Atom contained in this partition.
        public AtomIterator atomIterator()
        {
            // Reallocating an iterator every time is theoretically inefficient, but
            // most usage should never call this in the first place, and certainely
            // not more than once
            return new AtomIterator()
            {
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
                    return DeletionTime.LIVE;
                }

                public boolean hasNext()
                {
                    return false;
                }

                public Atom next()
                {
                    return null;
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }

                public void close()
                {
                }
            };
        }

        public AtomIterator atomIterator(Slices slices)
        {
            return atomIterator();
        }
    }
}
