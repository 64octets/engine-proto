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

    public int rows; // number of rows

    private final int clusteringSize;
    private ByteBuffer[] rowPaths;

    private final Column[] columns;
    // Positions of columns in the following up arrays. The positions for row 'i' are the ones
    // in [ i * columns.length, (i + 1) * columns.length]. The last index of the range represents
    // the index at which the row itself stops (or rather the first index of the elements of
    // row 'i+1'). In particular, the valu at index 'rows * (columns.length + 1)' will be the (valid)
    // size of the values, timestamps, ... arrays below.
    private int[] columnPositions;

    private final BitSet delFlags;
    private ByteBuffer[] values;
    private long[] timestamps;
    private int[] ttls;
    private long[] delTimes;
    private ByteBuffer[] collectionKeys;

    RowData(Layout layout, int rowsCapacity, int cellsCapacity)
    {
        this.clusteringSize = layout.clusteringSize();
        this.rowPaths = new ByteBuffer[rowsCapacity * clusteringSize];

        this.columns = layout.regularColumns();
        this.columnPositions = new int[(rowsCapacity * columns.length) + 1];

        this.delFlags = new BitSet(cellsCapacity);
        this.values = new ByteBuffer[cellsCapacity];
        this.timestamps = new long[cellsCapacity];
        this.ttls = new int[cellsCapacity];
        this.delTimes = new long[cellsCapacity];
        this.collectionKeys = layout.hasCollections() ? new ByteBuffer[cellsCapacity] : null;
    }

    public int clusteringSize()
    {
        return clusteringSize;
    }

    public ByteBuffer clusteringColumn(int row, int i)
    {
        return rowPaths[(row * clusteringSize) + i];
    }

    public int rows()
    {
        return rows;
    }

    public void setClusteringColumn(int row, int i, ByteBuffer value)
    {
        ensureCapacityForRow(row);
        rowPaths[(row * clusteringSize) + i] = value;
    }

    private int columnIdx(Column c)
    {
        for (int i = 0; i < columns.length; i++)
            if (columns[i].equals(c))
                return i;
        return -1;
    }

    public int startPosition(int row)
    {
        int base = row * columns.length;
        for (int i = 0; i < columns.length; i++)
        {
            int pos = columnPositions[base + i];
            if (pos >= 0)
                return pos;
        }
        throw new AssertionError("That row has no data. This should not happen");
    }

    public int endPosition(int row)
    {
        return startPosition(row+1);
    }

    public int cellsCount(int row)
    {
        return endPosition(row) - startPosition(row);
    }

    public int position(int row, Column c)
    {
        int idx = columnIdx(c);
        return idx < 0 ? -1 : columnPositions[(row * columns.length) + idx];
    }

    public Column columnForPosition(int row, int pos)
    {
        int base = row * columns.length;
        int last = -1;
        for (int i = base; i < columnPositions.length; i++)
        {
            int p = columnPositions[i];
            if (p == pos)
                return columns[i - base];
            else if (p > pos)
                return columns[last];
            else
                last = i - base;
        }
        throw new AssertionError();
    }

    public int size(int row, Column c)
    {
        int idx = columnIdx(c);
        if (idx < 0)
            return 0;

        int base = (row * columns.length) + idx;
        int basePos = columnPositions[base];
        if (basePos < 0)
            return 0;

        for (int i = base + 1; i < columnPositions.length; i++)
        {
            int pos = columnPositions[i];
            if (pos >= 0)
                return pos - basePos;
        }

        throw new AssertionError("The last element of columnPositions should always be positive");
    }

    public boolean isTombstone(int idx)
    {
        return delFlags.get(idx);
    }

    public ByteBuffer value(int idx)
    {
        return values[idx];
    }

    public long timestamp(int idx)
    {
        return timestamps[idx];
    }

    public int ttl(int idx)
    {
        return ttls[idx];
    }

    public long deletionTime(int idx)
    {
        return delTimes[idx];
    }

    public ByteBuffer key(int idx)
    {
        return collectionKeys == null ? null : collectionKeys[idx];
    }

    public void setTombstone(int idx)
    {
        ensureCapacityFor(idx);
        delFlags.set(idx);
    }

    public void setValue(int idx, ByteBuffer value)
    {
        ensureCapacityFor(idx);
        values[idx] = value;
    }

    public void setTimestamp(int idx, long tstamp)
    {
        ensureCapacityFor(idx);
        timestamps[idx] = tstamp;
    }

    public void setTTL(int idx, int ttl)
    {
        ensureCapacityFor(idx);
        ttls[idx] = ttl;
    }

    public void setDeletionTime(int idx, long deletionTime)
    {
        ensureCapacityFor(idx);
        delTimes[idx] = deletionTime;
    }

    public void setKey(int idx, ByteBuffer key)
    {
        ensureCapacityFor(idx);
        collectionKeys[idx] = key;
    }

    private void ensureCapacityForRow(int row)
    {
        int currentCapacity = rowPaths.length / clusteringSize;
        if (row < currentCapacity)
            return;

        int newCapacity = (currentCapacity * 3) / 2 + 1;

        rowPaths = Arrays.copyOf(rowPaths, newCapacity * clusteringSize);
        columnPositions = Arrays.copyOf(columnPositions, (newCapacity * columns.length) + 1);
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

    public WriteHelper createWriteHelper()
    {
        return new WriteHelper();
    }

    // Simple helper object to make simple to write rows into this RowData object.
    public class WriteHelper
    {
        private int pos;
        private int lastColumnIdx;

        private WriteHelper()
        {
            reset();
        }

        public int row()
        {
            return rows;
        }

        public int positionFor(Column c)
        {
            ++pos;

            // If lastColumnIdx already points to c, this means this is a collection cell
            // and not the first one. In any other case, this is the first cell for that
            // column and we need to mark the position.
            if (lastColumnIdx < 0 || !c.equals(columns[lastColumnIdx]))
            {
                int base = rows * columns.length;
                ++lastColumnIdx;
                // It could be we have to skip a few columns between the last written and this one
                while (!c.equals(columns[lastColumnIdx]))
                    columnPositions[base + (lastColumnIdx++)] = -1;

                columnPositions[base + lastColumnIdx] = pos;
            }

            return pos;
        }

        // called when a row is done
        public void rowDone()
        {
            for (int i = lastColumnIdx + 1; i < columns.length; i++)
                columnPositions[(rows * columns.length) + i] = -1;

            ++rows;
            // We write the next available position as the first value for the next row in
            // columnPositions in case this was the last row that will be written. If
            // it's not, it will just be overriden right away.
            columnPositions[rows * columns.length] = pos + 1;
            lastColumnIdx = -1;
        }

        public void reset()
        {
            rows = 0;
            pos = -1;
            lastColumnIdx = -1;
        }
    }
}
