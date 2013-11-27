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
import java.util.Comparator;

public abstract class AbstractRow implements Row
{
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;
    public static final int NO_TTL = 0;
    public static final long NO_LOCAL_DELETION_TIME = Long.MAX_VALUE;

    protected final Column[] columns;

    protected AbstractRow(Layout layout)
    {
        this.columns = layout.regularColumns();
    }

    // Must return the total number of cells for pos == columns.length
    protected abstract int indexForPos(int pos);
    protected abstract int nextValidPosAfter(int pos);

    protected abstract boolean isDeletedCell(int idx);
    protected abstract ByteBuffer value(int idx);
    protected abstract long timestamp(int idx);
    protected abstract int ttl(int idx);
    protected abstract long deletionTime(int idx);
    protected abstract ByteBuffer collectionKey(int idx);

    protected abstract void setDeletedCell(int idx);
    protected abstract void setValue(int idx, ByteBuffer value);
    protected abstract void setTimestamp(int idx, long tstamp);
    protected abstract void setTTL(int idx, int ttl);
    protected abstract void setDeletionTime(int idx, long deletionTime);
    protected abstract void setCollectionKey(int idx, ByteBuffer key);

    protected abstract Writer writer();

    protected int pos(Column c)
    {
        for (int i = 0; i < columns.length; i++)
            if (columns[i].equals(c))
                return i;
        return -1;
    }

    private int size(int pos)
    {
        if (pos < 0)
            return 0;
        int idx = indexForPos(pos);
        return idx < 0 ? 0 : indexForPos(nextValidPosAfter(pos)) - idx;
    }

    private int cellIdx(Column c)
    {
        int pos = pos(c);
        return pos < 0 ? -1 : indexForPos(pos);
    }

    /**
     * Merge two AbstractRow objects, assuming they do represent the same logical row
     * (i.e. that they share the same clustering prefix).
     */
    public static void merge(Layout layout, AbstractRow left, AbstractRow right, RowMerger merger, int now)
    {
        for (int i = 0; i < layout.clusteringSize(); i++)
            merger.setClusteringColumn(i, left.getClusteringColumn(i));

        for (int pos = 0; pos < left.columns.length; pos++)
        {
            Column c = left.columns[pos];
            int leftIdx = left.indexForPos(pos);
            int rightIdx = right.indexForPos(pos);
            if (leftIdx < 0 && rightIdx < 0)
                continue;

            if (c.isCollection())
            {
                Comparator<ByteBuffer> keyComparator = layout.collectionKeyComparator(c);
                int sizeLeft = left.size(pos);
                int sizeRight = right.size(pos);
                int iLeft = 0;
                int iRight = 0;
                while (iLeft < sizeLeft || iRight < sizeRight)
                {
                    ByteBuffer leftKey = left.collectionKey(leftIdx + iLeft);
                    ByteBuffer rightKey = right.collectionKey(rightIdx + iRight);
                    int cmp = iLeft >= sizeLeft ? 1 : (iRight >= sizeRight ? -1 : keyComparator.compare(leftKey, rightKey));
                    if (cmp < 0)
                    {
                        doCollectionMerge(merger, c, left, leftIdx + iLeft, leftKey, RowMerger.Resolution.ONLY_IN_LEFT);
                        ++iLeft;
                    }
                    else if (cmp > 0)
                    {
                        doCollectionMerge(merger, c, right, rightIdx + iRight, rightKey, RowMerger.Resolution.ONLY_IN_RIGHT);
                        ++iRight;
                    }
                    else
                    {
                        doMerge(merger, c, left, leftIdx + iLeft, leftKey, right, rightIdx + iRight, rightKey, now);
                        ++iLeft;
                        ++iRight;
                    }
                }
            }
            else
            {
                doMerge(merger, c, left, leftIdx, null, right, rightIdx, null, now);
            }
        }
        merger.closeRow();
    }

    private static void doMerge(RowMerger merger, Column c, AbstractRow left, int leftIdx, ByteBuffer leftKey, AbstractRow right, int rightIdx, ByteBuffer rightKey, int now)
    {
        AbstractRow toCopy;
        int idxCopy;
        ByteBuffer keyToCopy;
        RowMerger.Resolution resolution = mergeResolution(left, leftIdx, right, rightIdx, now);
        switch (resolution)
        {
            case ONLY_IN_RIGHT:
            case MERGED_FROM_RIGHT:
                toCopy = right;
                idxCopy = rightIdx;
                keyToCopy = rightKey;
                break;
            default:
                toCopy = left;
                idxCopy = leftIdx;
                keyToCopy = leftKey;
                break;
        }

        if (c.isCollection())
            doCollectionMerge(merger, c, toCopy, idxCopy, keyToCopy, resolution);
        else
            doRegularMerge(merger, c, toCopy, idxCopy, resolution);
    }

    private static void doRegularMerge(RowMerger merger, Column c, AbstractRow row, int idx, RowMerger.Resolution resolution)
    {
        merger.onRegularCell(c, row.isDeletedCell(idx), row.value(idx), row.timestamp(idx), row.ttl(idx), row.deletionTime(idx), resolution);
    }

    private static void doCollectionMerge(RowMerger merger, Column c, AbstractRow row, int idx, ByteBuffer key, RowMerger.Resolution resolution)
    {
        merger.onCollectionCell(c, key, row.isDeletedCell(idx), row.value(idx), row.timestamp(idx), row.ttl(idx), row.deletionTime(idx), resolution);
    }

    private static RowMerger.Resolution mergeResolution(AbstractRow left, int leftIdx, AbstractRow right, int rightIdx, int now)
    {
        if (leftIdx < 0 || (rightIdx >= 0 && right.cellOverwrites(rightIdx, left, leftIdx, now)))
            return leftIdx < 0 ? RowMerger.Resolution.ONLY_IN_RIGHT : RowMerger.Resolution.MERGED_FROM_RIGHT;
        else
            return rightIdx < 0 ? RowMerger.Resolution.ONLY_IN_LEFT : RowMerger.Resolution.MERGED_FROM_LEFT;
    }

    // TODO: deal with counters
    private boolean cellOverwrites(int idx, AbstractRow other, int otherIdx, int now)
    {
        if (timestamp(idx) < other.timestamp(otherIdx))
            return false;
        if (timestamp(idx) > other.timestamp(otherIdx))
            return true;

        // Tombstones take precedence (if the other is also a tombstone, it doesn't matter).
        if (isDeletedCell(idx) || deletionTime(idx) <= now)
            return true;

        // Same timestamp, none is a tombstone, compare values
        return value(idx).compareTo(other.value(otherIdx)) < 0;
    }

    public int cellsCount()
    {
        return indexForPos(columns.length);
    }

    public boolean has(Column c)
    {
        return cellIdx(c) >= 0;
    }

    // For non-collection columns
    public boolean isDeleted(Column c, int now)
    {
        int idx = cellIdx(c);
        return idx < 0 ? false : isDeletedCell(idx) || deletionTime(idx) <= now;
    }

    public ByteBuffer value(Column c)
    {
        int idx = cellIdx(c);
        return idx < 0 ? null : value(idx);
    }

    public long timestamp(Column c)
    {
        int idx = cellIdx(c);
        return idx < 0 ? NO_TIMESTAMP : timestamp(idx);
    }

    public int ttl(Column c)
    {
        int idx = cellIdx(c);
        return idx < 0 ? NO_TTL : ttl(idx);
    }

    public long localDeletionTime(Column c)
    {
        int idx = cellIdx(c);
        return idx < 0 ? NO_LOCAL_DELETION_TIME : deletionTime(idx);
    }

    public long timestampOfLastDelete(Column c)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // Collections columns
    public int size(Column c)
    {
        return size(pos(c));
    }

    public boolean isDeleted(Column c, int i, int now)
    {
        int idx = cellIdx(c) + i;
        return idx < 0 ? false : isDeletedCell(idx) || deletionTime(idx) <= now;
    }

    public ByteBuffer key(Column c, int i)
    {
        int idx = cellIdx(c);
        return idx < 0 ? null : collectionKey(idx + i);
    }

    public ByteBuffer value(Column c, int i)
    {
        int idx = cellIdx(c);
        return idx < 0 ? null : value(idx + i);
    }

    public long timestamp(Column c, int i)
    {
        int idx = cellIdx(c);
        return idx < 0 ? NO_TIMESTAMP : timestamp(idx + i);
    }

    public int ttl(Column c, int i)
    {
        int idx = cellIdx(c);
        return idx < 0 ? NO_TTL : ttl(idx + i);
    }

    public long localDeletionTime(Column c, int i)
    {
        int idx = cellIdx(c);
        return idx < 0 ? NO_LOCAL_DELETION_TIME : deletionTime(idx + i);
    }


    /**
     * Convenient class to write/merge into an AbstractRow.
     */
    protected abstract class Writer implements RowMerger
    {
        private int pos;
        private int idx = -1;
        private boolean inCollection;

        protected abstract void skip(int pos);
        protected abstract void newColumn(int pos, int idx);
        protected abstract void newCell(int idx);

        protected abstract void close(int idx);

        protected void advanceTo(Column c)
        {
            if (inCollection)
                ++pos;

            inCollection = false;
            ++idx;

            while (!columns[pos].equals(c))
                skip(pos++);

            newColumn(pos++, idx);
            newCell(idx);
        }

        protected void advanceTo(Column c, ByteBuffer key)
        {
            ++idx;

            if (!columns[pos].equals(c))
            {
                inCollection = false;
                do
                    skip(pos++);
                while (!columns[pos].equals(c));
            }

            if (!inCollection)
            {
                newColumn(pos, idx);
                inCollection = true;
            }

            newCell(idx);
        }

        protected void reset()
        {
            this.pos = 0;
            this.idx = 0;
            this.inCollection = false;
        }

        public void closeRow()
        {
            if (inCollection)
                ++pos;
            for (int i = pos; i < columns.length; i++)
                skip(i);
            close(idx + 1);
        }

        public void addCell(Column c, boolean isDeletedCell, ByteBuffer value, long timestamp, int ttl, long deletionTime)
        {
            advanceTo(c);

            if (isDeletedCell)
                setDeletedCell(idx);
            setValue(idx, value);
            setTimestamp(idx, timestamp);
            setTTL(idx, ttl);
            setDeletionTime(idx, deletionTime);
        }

        public void addCollectionCell(Column c, ByteBuffer key, boolean isDeletedCell, ByteBuffer value, long timestamp, int ttl, long deletionTime)
        {
            advanceTo(c, key);

            if (isDeletedCell)
                setDeletedCell(idx);
            setCollectionKey(idx, key);
            setValue(idx, value);
            setTimestamp(idx, timestamp);
            setTTL(idx, ttl);
            setDeletionTime(idx, deletionTime);
        }

        // Ignores the resolution
        public void onRegularCell(Column c, boolean isDeletedCell, ByteBuffer value, long timestamp, int ttl, long deletionTime, Resolution resolution)
        {
            addCell(c, isDeletedCell, value, timestamp, ttl, deletionTime);
        }

        // Ignores the resolution
        public void onCollectionCell(Column c, ByteBuffer collectionKey, boolean isDeletedCell, ByteBuffer value, long timestamp, int ttl, long deletionTime, Resolution resolution)
        {
            addCollectionCell(c, collectionKey, isDeletedCell, value, timestamp, ttl, deletionTime);
        }
    }
}
