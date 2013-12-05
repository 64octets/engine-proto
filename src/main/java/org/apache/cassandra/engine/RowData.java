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
// TODO: use OpenBitSet?
import java.util.Arrays;
import java.util.BitSet;

import com.google.common.base.Objects;
import com.google.common.collect.UnmodifiableIterator;

/**
 * A container for arrays containing one or more row data.
 *
 * This class is mainly here for code reuse.
 */
public class RowData
{
    // TODO: currently, when we need capacity, we just allocate bigger arrays and copy everything.
    // For small number of rows this is probably fine, but the copy could be costly for bigger
    // partitions. On strategy could be to use fixed size block of data. When we need more data
    // we'd just allocate a new block. We could even imagine to have a pool of blocks to reduce
    // allocations even more.

    public final Layout metadata;

    public int rows; // number of rows

    private ByteBuffer[] rowPaths;

    private final Column[] columns;

    // Cells start and end positions for each column of each row.
    // Note: we record both start and end positions which allows to support inserting cells
    // in unsorted order (for a given row, useful for the write path). It also simplify the
    // implementation a bit. In the cases where cells are added in sorted order  anyway (the
    // read path), it would be possible to use only one array for the start position and the
    // end position would be computed by used the next non-negative position in the array. We
    // keep it simple for now though.
    private int[] columnStartPositions;
    private int[] columnEndPositions;

    private final BitSet delFlags;
    private ByteBuffer[] values;
    private long[] timestamps;
    private int[] ttls;
    private long[] delTimes;
    private ByteBuffer[] collectionKeys;

    RowData(Layout metadata, int rowsCapacity, int cellsCapacity)
    {
        this.metadata = metadata;
        this.rowPaths = new ByteBuffer[rowsCapacity * metadata.clusteringSize()];

        // TODO: instead of using all columns all the time, we could collect the column actually
        // used by the iterators that will be use to populate this as an optimization for tables
        // that have lots of columns but most are rarely set.
        this.columns = metadata.regularColumns();
        this.columnStartPositions = new int[rowsCapacity * columns.length];
        this.columnEndPositions = new int[rowsCapacity * columns.length];

        this.delFlags = new BitSet(cellsCapacity);
        this.values = new ByteBuffer[cellsCapacity];
        this.timestamps = new long[cellsCapacity];
        this.ttls = new int[cellsCapacity];
        this.delTimes = new long[cellsCapacity];
        this.collectionKeys = metadata.hasCollections() ? new ByteBuffer[cellsCapacity] : null;
    }

    public Layout metadata()
    {
        return metadata;
    }

    public int clusteringSize()
    {
        return metadata.clusteringSize();
    }

    public ByteBuffer clusteringColumn(int row, int i)
    {
        return rowPaths[(row * clusteringSize()) + i];
    }

    public void setClusteringColumn(int row, int i, ByteBuffer value)
    {
        ensureCapacityForRow(row);
        rowPaths[(row * clusteringSize()) + i] = value;
    }

    public int rows()
    {
        return rows;
    }

    public int cells()
    {
        return columnEndPositions[(rows * columns.length) - 1];
    }

    private int findColumn(Column c)
    {
        for (int i = 0; i < columns.length; i++)
            if (columns[i].equals(c))
                return i;
        throw new AssertionError();
    }

    public int cellsCount(int row)
    {
        int base = row * columns.length;

        int first = -1;
        for (int i = 0; i < columns.length; i++)
        {
            int idx = base + i;
            if (columnStartPositions[idx] < columnEndPositions[idx])
            {
                first = columnStartPositions[idx];
                break;
            }
        }

        if (first == -1)
            return 0;

        int last = -1;
        for (int i = columns.length - 1; i >= 0; i--)
        {
            int idx = base + i;
            if (columnStartPositions[idx] < columnEndPositions[idx])
            {
                last = columnEndPositions[idx];
                break;
            }
        }

        assert last != -1;
        return last - first;
    }

    public int size(int row, Column c)
    {
        int idx = findColumn(c);
        int start = columnStartPositions[idx];
        int end = columnEndPositions[idx];
        return start < end ? end - start : 0;
    }

    private void ensureCapacityForRow(int row)
    {
        int currentCapacity = rowPaths.length / clusteringSize();
        if (row < currentCapacity)
            return;

        int newCapacity = (currentCapacity * 3) / 2 + 1;

        rowPaths = Arrays.copyOf(rowPaths, newCapacity * clusteringSize());
        columnStartPositions = Arrays.copyOf(columnStartPositions, newCapacity * columns.length);
        columnEndPositions = Arrays.copyOf(columnEndPositions, newCapacity * columns.length);
    }

    private void ensureCapacityFor(int idxToSet)
    {
        if (idxToSet < values.length)
            return;

        int newSize = (values.length * 3) / 2 + 1;

        values = Arrays.copyOf(values, newSize);
        timestamps = Arrays.copyOf(timestamps, newSize);
        ttls = Arrays.copyOf(ttls, newSize);
        delTimes = Arrays.copyOf(delTimes, newSize);
        if (collectionKeys != null)
            collectionKeys = Arrays.copyOf(collectionKeys, newSize);
    }

    public ReusableCell createCell()
    {
        return new ReusableCell();
    }

    public class ReusableCell extends UnmodifiableIterator<Cell> implements Cell, Rows.Writer
    {
        private int row;
        private int columnIdx;
        private int position = -1;

        private boolean rowWritten;
        private boolean ready;

        public int currentRow()
        {
            return row;
        }

        public void setToRow(int row)
        {
            this.row = row;
            this.columnIdx = row * columns.length;

            while (columnStartPositions[columnIdx] >= columnEndPositions[columnIdx])
                ++columnIdx;

            this.position = columnStartPositions[columnIdx] - 1;
        }

        public void setToCell(Column c, int i)
        {
            columnIdx = (row * columns.length) + findColumn(c);
            position = columnStartPositions[columnIdx] + i;
        }

        // Note: hasNext() actually ends up preparing the next element in the process. However
        // we don't reuse guava AbstractIterator because it's not "reusable".
        public boolean hasNext()
        {
            ready = true;

            if (row >= rows)
                return false;

            ++position;
            if (position < columnEndPositions[columnIdx])
                return true;

            ++columnIdx;
            while (columnIdx < columnStartPositions.length && columnStartPositions[columnIdx] >= columnEndPositions[columnIdx])
                ++columnIdx;

            // We may have passed to the next row
            if (columnIdx < (row + 1) * columns.length)
                return true;

            ++row;
            return false;
        }

        public Cell next()
        {
            if (!ready)
                hasNext();
            ready = false;
            return this;
        }

        public Column column()
        {
            return columns[columnIdx % columns.length];
        }

        public boolean isDeleted(int now)
        {
            return isTombstone() || localDeletionTime() <= now;
        }

        public boolean isTombstone()
        {
            return delFlags.get(position);
        }

        public ByteBuffer value()
        {
            return values[position];
        }

        public long timestamp()
        {
            return timestamps[position];
        }

        public int ttl()
        {
            return ttls[position];
        }

        public long localDeletionTime()
        {
            return delTimes[position];
        }

        public ByteBuffer key()
        {
            return collectionKeys[position];
        }

        public void setClusteringColumn(int i, ByteBuffer value)
        {
            RowData.this.setClusteringColumn(row, i, value);
        }

        public void addCell(Cell cell)
        {
            addCell(cell.column(), cell.isTombstone(), cell.key(), cell.value(), cell.timestamp(), cell.ttl(), cell.localDeletionTime());
        }

        public void addCell(Column c, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime)
        {
            ++position;
            ensureCapacityFor(position);

            int newColIdx = (row * columns.length) + findColumn(c);

            // Set the end position of the previous cell and the start of that new one if we're not
            // just written more cells for a started collection, i.e. if this is the first cell written
            // in this row or it's not the same column that the previous one.
            if (!rowWritten || newColIdx != columnIdx)
            {
                columnEndPositions[columnIdx] = position;
                columnStartPositions[newColIdx] = position;
                columnIdx = newColIdx;
            }

            if (isTombstone)
                delFlags.set(position);
            collectionKeys[position] = key;
            values[position] = value;
            timestamps[position] = timestamp;
            ttls[position] = ttl;
            delTimes[position] = deletionTime;

            rowWritten = true;
        }

        // called when a row is done
        public void done()
        {
            columnEndPositions[columnIdx] = position + 1;
            ++row;
            ++rows;
            rowWritten = false;
            ready = false;
        }

        public void reset()
        {
            row = 0;
            columnIdx = 0;
            position = -1;
        }

        public void clear()
        {
            reset();
            Arrays.fill(columnStartPositions, 0);
            Arrays.fill(columnEndPositions, 0);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Cell))
                return false;

            Cell that = (Cell)other;
            return Objects.equal(this.column(), that.column())
                && Objects.equal(this.key(), that.key())
                && Objects.equal(this.value(), that.value())
                && this.isTombstone() == that.isTombstone()
                && this.timestamp() == that.timestamp()
                && this.ttl() == that.ttl()
                && this.localDeletionTime() == that.localDeletionTime();
        }
    }
}
