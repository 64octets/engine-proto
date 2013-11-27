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
import java.util.Arrays;
import java.util.BitSet;


// TODO: We should specialize for dense rows, where we have only one cell per row.
public class ReusableRow extends AbstractRow
{
    private int[] columnIndexes;

    private ByteBuffer[] rowPath;

    private final BitSet delFlags;
    private ByteBuffer[] values;
    private long[] timestamps;
    private int[] ttls;
    private long[] delTimes;
    private ByteBuffer[] collectionKeys;

    private ConcreteWriter writer;

    public ReusableRow(Layout layout, int initialCapacity, boolean hasCollections)
    {
        super(layout);
        this.columnIndexes = new int[columns.length + 1];

        this.delFlags = new BitSet(initialCapacity);
        this.values = new ByteBuffer[initialCapacity];
        this.timestamps = new long[initialCapacity];
        this.ttls = new int[initialCapacity];
        this.delTimes = new long[initialCapacity];
        this.collectionKeys = hasCollections ? new ByteBuffer[initialCapacity] : null;
    }

    public int clusteringSize()
    {
        return rowPath.length;
    }

    public ByteBuffer getClusteringColumn(int i)
    {
        return rowPath[i];
    }

    protected int indexForPos(int pos)
    {
        return columnIndexes[pos];
    }

    protected int nextValidPosAfter(int pos)
    {
        for (int i = pos + 1; i < columnIndexes.length; i++)
            if (columnIndexes[i] >= 0)
                return i;

        throw new AssertionError("The last element of columnIndexes should always be positive");
    }

    protected boolean isDeletedCell(int idx)
    {
        return delFlags.get(idx);
    }

    protected ByteBuffer value(int idx)
    {
        return values[idx];
    }

    protected long timestamp(int idx)
    {
        return timestamps[idx];
    }

    protected int ttl(int idx)
    {
        return ttls[idx];
    }

    protected long deletionTime(int idx)
    {
        return delTimes[idx];
    }

    protected ByteBuffer collectionKey(int idx)
    {
        return collectionKeys[idx];
    }

    protected void setDeletedCell(int idx)
    {
        delFlags.set(idx);
    }

    protected void setValue(int idx, ByteBuffer value)
    {
        values[idx] = value;
    }

    protected void setTimestamp(int idx, long tstamp)
    {
        timestamps[idx] = tstamp;
    }

    protected void setTTL(int idx, int ttl)
    {
        ttls[idx] = ttl;
    }

    protected void setDeletionTime(int idx, long deletionTime)
    {
        delTimes[idx] = deletionTime;
    }

    protected void setCollectionKey(int idx, ByteBuffer key)
    {
        collectionKeys[idx] = key;
    }

    private void growArrays()
    {
        int newSize = (values.length * 3) / 2 + 1;

        values = Arrays.copyOf(values, newSize);
        timestamps = Arrays.copyOf(timestamps, newSize);
        ttls = Arrays.copyOf(ttls, newSize);
        delTimes = Arrays.copyOf(delTimes, newSize);
        if (collectionKeys != null)
            collectionKeys = Arrays.copyOf(collectionKeys, newSize);
    }

    public Writer writer()
    {
        if (writer == null)
            writer = new ConcreteWriter();
        else
            writer.reset();
        return writer;
    }

    private class ConcreteWriter extends Writer
    {
        public void setClusteringColumn(int i, ByteBuffer value)
        {
            rowPath[i] = value;
        }

        protected void skip(int pos)
        {
            columnIndexes[pos] = -1;
        }

        // Return the index to use as base for that column
        protected void newColumn(int pos, int idx)
        {
            columnIndexes[pos] = idx++;
        }

        protected void close(int idx)
        {
            columnIndexes[columns.length] = idx;
        }

        protected void newCell(int idx)
        {
            if (idx >= values.length)
                growArrays();
        }

    }
}
