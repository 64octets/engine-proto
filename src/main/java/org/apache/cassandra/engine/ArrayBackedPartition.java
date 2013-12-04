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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.engine.utils.*;

/**
 * A partition backed by arrays.
 *
 * An ArrayBackedPartition is an "almost" immutable object. The one exception
 * being the deletion infos that can be muted.
 *
 * TODO: It's not meant to me muted so we should maybe have some
 * MutableDeletionInfo but keep DeletionInfo immutable.
 */
public class ArrayBackedPartition implements Partition
{
    // TODO: We might want specialized versions for dense and/or non-composite layouts. It's possibly
    // less important than for say ReusableRow though

    private final Layout metadata; 
    private final DecoratedKey partitionKey;
    //private final ByteBuffer[] partitionKeyComponents;

    private final DeletionInfo deletion;

    private final RowData data;

    private ArrayBackedPartition(Layout metadata, DecoratedKey partitionKey, int rowsCapacity, int cellsCapacity)
    {
        this.metadata = metadata;

        this.partitionKey = partitionKey;
        //this.partitionKeyComponents = metadata.splitPartitionKey(partitionKey);

        this.deletion = DeletionInfo.live();

        this.data = new RowData(metadata, rowsCapacity, cellsCapacity);
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
        return deletion;
    }

    // No values (even deleted), live deletion infos
    public boolean isEmpty()
    {
        return deletion.isLive() && data.rows() == 0;
    }

    public int rowCount()
    {
        return data.rows();
    }

    // Use sparingly, prefer iterator() when possible to save allocations
    public Row findRow(RowPath path)
    {
        RowCursor wrapper = new RowCursor();
        return Cursors.moveTo(path, wrapper) ? wrapper : null;
    }

    //public Iterator<Row> iterator()
    //{
    //    return new IndexBasedIterator<Row>(new RowCursor());
    //}

    //public AtomIterator atomIterator(Slices slices)
    //{
    //    // TODO: can have a specialized version if slices selects all
    //    return new PartitionAtomIterator(slices);
    //}

    public AtomIterator atomIterator()
    {
        return new PartitionAtomIterator();
    }

    private CursorBasedIterator<RowCursor> rowIterator()
    {
        return new CursorBasedIterator<RowCursor>(new RowCursor());
    }

    public static Partition accumulate(AtomIterator iterator, int rowsCapacity, int cellsCapacity)
    {
        if (!iterator.hasNext())
            return Partitions.empty(iterator.metadata(), iterator.getPartitionKey());

        ArrayBackedPartition partition = new ArrayBackedPartition(iterator.metadata(), iterator.getPartitionKey(), rowsCapacity, cellsCapacity);
        partition.deletion.add(iterator.partitionLevelDeletion());

        RowCursor currentRow = partition.new RowCursor();
        AbstractRow.Writer writer = currentRow.writer();

        while (iterator.hasNext())
        {
            Atom next = iterator.next();
            switch (next.kind())
            {
                case ROW:
                    writer.copyRow((Row)next);
                    ++currentRow.row;
                    break;
                case RANGE_TOMBSTONE:
                    partition.deletion.add((RangeTombstone)next, partition.comparator());
                    break;
                case COLLECTION_TOMBSTONE:
                    // TODO
                    break;
            }
        }
        return partition;
    }

    private class RowCursor extends AbstractRow implements Cursor
    {
        private int row;
        private AbstractRow.Writer writer;

        public int row()
        {
            return row;
        }

        public Writer writer()
        {
            if (writer == null)
                writer = new AbstractRow.Writer(data);
            return writer;
        }

        protected RowData data()
        {
            return data;
        }

        public void position(int i)
        {
            row = i;
        }

        public int position()
        {
            return row;
        }

        public int limit()
        {
            return data.rows();
        }

        public ClusteringComparator comparator()
        {
            return metadata.comparator();
        }
    }

    private class PartitionAtomIterator extends AbstractIterator<Atom> implements AtomIterator
    {
        // TODO needs to handle collection tombstones ...
        private final CursorBasedIterator<RangeTombstoneList.Cursor> tombstoneIter;
        private final CursorBasedIterator<RowCursor> rowIter;

        //public PartitionAtomIterator(Slices slices)
        public PartitionAtomIterator()
        {
            this.tombstoneIter = deletion.rangeIterator();
            //this.rowIter = slices.makeIterator(new IndexedCellIterator());
            this.rowIter = rowIterator();
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
            return deletion.getTopLevelDeletion();
        }

        protected Atom computeNext()
        {
            if (!tombstoneIter.hasNext())
                return rowIter.hasNext() ? rowIter.next() : endOfData();

            if (!rowIter.hasNext())
                return tombstoneIter.next();

            RangeTombstone tombstone = tombstoneIter.next();
            Row row = rowIter.next();

            if (comparator().atomComparator().compare(tombstone, row) < 0) {
                rowIter.rewindOne();
                return tombstone;
            } else {
                tombstoneIter.rewindOne();
                return row;
            }
        }

        public void close()
        {
        }
    }
}
