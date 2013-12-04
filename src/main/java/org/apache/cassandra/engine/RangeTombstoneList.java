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
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.engine.utils.CursorBasedIterator;
import org.apache.cassandra.engine.utils.Cursors;

/**
 * Data structure holding the range tombstones of a ColumnFamily.
 * <p>
 * This is essentially a sorted list of non-overlapping (tombstone) ranges.
 * <p>
 * A range tombstone has 4 elements: the start and end of the range covered,
 * and the deletion infos (markedAt timestamp and local deletion time). The
 * markedAt timestamp is what define the priority of 2 overlapping tombstones.
 * That is, given 2 tombstones [0, 10]@t1 and [5, 15]@t2, then if t2 > t1 (and
 * are the tombstones markedAt values), the 2nd tombstone take precedence over
 * the first one on [5, 10]. If such tombstones are added to a RangeTombstoneList,
 * the range tombstone list will store them as [[0, 5]@t1, [5, 15]@t2].
 * <p>
 * The only use of the local deletion time is to know when a given tombstone can
 * be purged, which will be done by the purge() method.
 */
public class RangeTombstoneList
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTombstoneList.class);

    private final ClusteringComparator comparator;

    // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
    // use a List for starts and ends, but having arrays everywhere is almost simpler.
    private ClusteringPrefix[] starts;
    private ClusteringPrefix[] ends;
    private long[] markedAts;
    private int[] delTimes;

    private int size;

    private RangeTombstoneList(ClusteringComparator comparator, ClusteringPrefix[] starts, ClusteringPrefix[] ends, long[] markedAts, int[] delTimes, int size)
    {
        assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimes.length;
        this.comparator = comparator;
        this.starts = starts;
        this.ends = ends;
        this.markedAts = markedAts;
        this.delTimes = delTimes;
        this.size = size;
    }

    public RangeTombstoneList(ClusteringComparator comparator, int capacity)
    {
        this(comparator, new ClusteringPrefix[capacity], new ClusteringPrefix[capacity], new long[capacity], new int[capacity], 0);
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return size;
    }

    public ClusteringComparator comparator()
    {
        return comparator;
    }

    public RangeTombstoneList copy()
    {
        return new RangeTombstoneList(comparator,
                                      Arrays.copyOf(starts, size),
                                      Arrays.copyOf(ends, size),
                                      Arrays.copyOf(markedAts, size),
                                      Arrays.copyOf(delTimes, size),
                                      size);
    }

    public void add(RangeTombstone tombstone)
    {
        add(tombstone.min(), tombstone.max(), tombstone.delTime().markedForDeleteAt(), tombstone.delTime().localDeletionTime());
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case), 
     * but it doesn't assume it.
     */
    public void add(ClusteringPrefix start, ClusteringPrefix end, long markedAt, int delTime)
    {
        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTime);
            return;
        }

        int c = comparator.compare(ends[size-1], start);

        // Fast path if we add in sorted order
        if (c <= 0)
        {
            addInternal(size, start, end, markedAt, delTime);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            int pos = Arrays.binarySearch(ends, 0, size, start, comparator);
            insertFrom((pos >= 0 ? pos+1 : -pos-1), start, end, markedAt, delTime);
        }
    }

    /**
     * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
     */
    public void addAll(RangeTombstoneList tombstones)
    {
        if (tombstones.isEmpty())
            return;

        if (isEmpty())
        {
            copyArrays(tombstones, this);
            return;
        }

        /*
         * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
         * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, then
         * calling add() will be faster, otherwise it's merging that will be faster.
         *
         * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
         * tombstones, while the CF we're adding it to (the one in the memtable) has many. In that case, using add() is
         * likely going to be faster.
         *
         * In other cases however, like when diffing responses from multiple nodes, the tombstone lists we "merge" will
         * be likely sized, so using add() might be a bit inefficient.
         *
         * Roughly speaking (this ignore the fact that updating an element is not exactly constant but that's not a big
         * deal), if n is the size of this list and m is tombstones size, merging is O(n+m) while using add() is O(m*log(n)).
         *
         * But let's not crank up a logarithm computation for that. Long story short, merging will be a bad choice only
         * if this list size is lot bigger that the other one, so let's keep it simple.
         */
        if (size > 10 * tombstones.size)
        {
            for (int i = 0; i < tombstones.size; i++)
                add(tombstones.starts[i], tombstones.ends[i], tombstones.markedAts[i], tombstones.delTimes[i]);
        }
        else
        {
            int i = 0;
            int j = 0;
            while (i < size && j < tombstones.size)
            {
                if (comparator.compare(tombstones.starts[j], ends[i]) < 0)
                {
                    insertFrom(i, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Addds the remaining ones from tombstones if any (note that addInternal will increment size if relevant).
            for (; j < tombstones.size; j++)
                addInternal(size, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(Clusterable name, long timestamp)
    {
        Cursor cursor = searchInternal(name);
        return cursor != null && cursor.delTime().markedForDeleteAt() >= timestamp;
    }

    /**
     * Returns a new {@link InOrderTester}.
     */
    InOrderTester inOrderTester()
    {
        return new InOrderTester();
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime search(Clusterable name)
    {
        Cursor cursor = searchInternal(name);
        return cursor == null ? null : cursor.delTime();
    }

    private Cursor searchInternal(Clusterable name)
    {
        if (isEmpty())
            return null;

        Cursor cursor = new Cursor();
        if (Cursors.moveTo(name, cursor))
        {
            // We're exactly on an interval start. The one subtility is that we need to check if
            // the previous is not equal to us and doesn't have a higher marked at
            if (cursor.idx > 0 && comparator.compare(name, ends[cursor.idx-1]) == 0 && markedAts[cursor.idx-1] > cursor.delTime().markedForDeleteAt())
                --cursor.idx;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            --cursor.idx;
            if (cursor.idx < 0 || comparator.compare(name, cursor.max()) > 0)
                return null;
        }
        return cursor;
    }

    public long minMarkedAt()
    {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < size; i++)
            min = Math.min(min, markedAts[i]);
        return min;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < size; i++)
            max = Math.max(max, markedAts[i]);
        return max;
    }

    public void updateAllTimestamp(long timestamp)
    {
        for (int i = 0; i < size; i++)
            markedAts[i] = timestamp;
    }

    /**
     * Removes all range tombstones whose local deletion time is older than gcBefore.
     */
    public void purge(int gcBefore)
    {
        int j = 0;
        for (int i = 0; i < size; i++)
        {
            if (delTimes[i] >= gcBefore)
                setInternal(j++, starts[i], ends[i], markedAts[i], delTimes[i]);
        }
        size = j;
    }

    /**
     * Returns whether {@code purge(gcBefore)} would remove something or not.
     */
    public boolean hasIrrelevantData(int gcBefore)
    {
        for (int i = 0; i < size; i++)
        {
            if (delTimes[i] < gcBefore)
                return true;
        }
        return false;
    }

    public CursorBasedIterator<Cursor> iterator()
    {
        return new CursorBasedIterator<Cursor>(new Cursor());
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof RangeTombstoneList))
            return false;
        RangeTombstoneList that = (RangeTombstoneList)o;
        if (size != that.size)
            return false;

        for (int i = 0; i < size; i++)
        {
            if (!starts[i].equals(that.starts[i]))
                return false;
            if (!ends[i].equals(that.ends[i]))
                return false;
            if (markedAts[i] != that.markedAts[i])
                return false;
            if (delTimes[i] != that.delTimes[i])
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = size;
        for (int i = 0; i < size; i++)
        {
            result += starts[i].hashCode() + ends[i].hashCode();
            result += (int)(markedAts[i] ^ (markedAts[i] >>> 32));
            result += delTimes[i];
        }
        return result;
    }

    private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst)
    {
        dst.grow(src.size);
        System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
        System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
        System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
        System.arraycopy(src.delTimes, 0, dst.delTimes, 0, src.size);
        dst.size = src.size;
    }

    /*
     * Inserts a new element starting at index i. This method assumes that i is the insertion point
     * in term of intervals for start:
     *    ends[i-1] <= start < ends[i]
     */
    private void insertFrom(int i, ClusteringPrefix start, ClusteringPrefix end, long markedAt, int delTime)
    {
        while (i < size)
        {
            assert i == 0 || comparator.compare(start, ends[i-1]) >= 0;
            assert i >= size || comparator.compare(start, ends[i]) < 0;

            // Do we overwrite the current element?
            if (markedAt > markedAts[i])
            {
                // We do overwrite.

                // First deal with what might come before the newly added one.
                if (comparator.compare(starts[i], start) < 0)
                {
                    addInternal(i, starts[i], start, markedAts[i], delTimes[i]);
                    i++;
                    // We don't need to do the following line, but in spirit that's what we want to do
                    // setInternal(i, start, ends[i], markedAts, delTime])
                }

                // now, start <= starts[i]

                // If the new element stops before the current one, insert it and
                // we're done
                if (comparator.compare(end, starts[i]) <= 0)
                {
                    addInternal(i, start, end, markedAt, delTime);
                    return;
                }

                // Do we overwrite the current element fully?
                int cmp = comparator.compare(ends[i], end);
                if (cmp <= 0)
                {
                    // We do overwrite fully:
                    // update the current element until it's end and continue
                    // on with the next element (with the new inserted start == current end).

                    // If we're on the last element, we can optimize
                    if (i == size-1)
                    {
                        setInternal(i, start, end, markedAt, delTime);
                        return;
                    }

                    setInternal(i, start, ends[i], markedAt, delTime);
                    if (cmp == 0)
                        return;

                    start = ends[i];
                    i++;
                }
                else
                {
                    // We don't ovewrite fully. Insert the new interval, and then update the now next
                    // one to reflect the not overwritten parts. We're then done.
                    addInternal(i, start, end, markedAt, delTime);
                    i++;
                    setInternal(i, end, ends[i], markedAts[i], delTimes[i]);
                    return;
                }
            }
            else
            {
                // we don't overwrite the current element

                // If the new interval starts before the current one, insert that new interval
                if (comparator.compare(start, starts[i]) < 0)
                {
                    // If we stop before the start of the current element, just insert the new
                    // interval and we're done; otherwise insert until the beginning of the
                    // current element
                    if (comparator.compare(end, starts[i]) <= 0)
                    {
                        addInternal(i, start, end, markedAt, delTime);
                        return;
                    }
                    addInternal(i, start, starts[i], markedAt, delTime);
                    i++;
                }

                // After that, we're overwritten on the current element but might have
                // some residual parts after ...

                // ... unless we don't extend beyond it.
                if (comparator.compare(end, ends[i]) <= 0)
                    return;

                start = ends[i];
                i++;
            }
        }

        // If we got there, then just insert the remainder at the end
        addInternal(i, start, end, markedAt, delTime);
    }

    private int capacity()
    {
        return starts.length;
    }

    /*
     * Adds the new tombstone at index i, growing and/or moving elements to make room for it.
     */
    private void addInternal(int i, ClusteringPrefix start, ClusteringPrefix end, long markedAt, int delTime)
    {
        assert i >= 0;

        if (size == capacity())
            growToFree(i);
        else if (i < size)
            moveElements(i);

        setInternal(i, start, end, markedAt, delTime);
        size++;
    }

    /*
     * Grow the arrays, leaving index i "free" in the process.
     */
    private void growToFree(int i)
    {
        int newLength = (capacity() * 3) / 2 + 1;
        grow(i, newLength);
    }

    /*
     * Grow the arrays to match newLength capacity.
     */
    private void grow(int newLength)
    {
        if (capacity() < newLength)
            grow(-1, newLength);
    }

    private void grow(int i, int newLength)
    {
        starts = grow(starts, size, newLength, i);
        ends = grow(ends, size, newLength, i);
        markedAts = grow(markedAts, size, newLength, i);
        delTimes = grow(delTimes, size, newLength, i);
    }

    private static ClusteringPrefix[] grow(ClusteringPrefix[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        ClusteringPrefix[] newA = new ClusteringPrefix[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static long[] grow(long[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        long[] newA = new long[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static int[] grow(int[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        int[] newA = new int[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    /*
     * Move elements so that index i is "free", assuming the arrays have at least one free slot at the end.
     */
    private void moveElements(int i)
    {
        if (i >= size)
            return;

        System.arraycopy(starts, i, starts, i+1, size - i);
        System.arraycopy(ends, i, ends, i+1, size - i);
        System.arraycopy(markedAts, i, markedAts, i+1, size - i);
        System.arraycopy(delTimes, i, delTimes, i+1, size - i);
    }

    private void setInternal(int i, ClusteringPrefix start, ClusteringPrefix end, long markedAt, int delTime)
    {
        starts[i] = start;
        ends[i] = end;
        markedAts[i] = markedAt;
        delTimes[i] = delTime;
    }

    /**
     * This object allow testing whether a given column (name/timestamp) is deleted
     * or not by this RangeTombstoneList, assuming that the column given to this
     * object are passed in (comparator) sorted order.
     *
     * This is more efficient that calling RangeTombstoneList.isDeleted() repeatedly
     * in that case since we're able to take the sorted nature of the RangeTombstoneList
     * into account.
     */
    public class InOrderTester
    {
        private int idx;

        public boolean isDeleted(Clusterable name, long timestamp)
        {
            while (idx < size)
            {
                int cmp = comparator.compare(name, starts[idx]);
                if (cmp == 0)
                {
                    // As for searchInternal, we need to check the previous end
                    if (idx > 0 && comparator.compare(name, ends[idx-1]) == 0 && markedAts[idx-1] > markedAts[idx])
                        return markedAts[idx-1] >= timestamp;
                    else
                        return markedAts[idx] >= timestamp;
                }
                else if (cmp < 0)
                {
                    return false;
                }
                else
                {
                    if (comparator.compare(name, ends[idx]) <= 0)
                        return markedAts[idx] >= timestamp;
                    else
                        idx++;
                }
            }
            return false;
        }
    }

    public class Cursor extends RangeTombstone implements org.apache.cassandra.engine.utils.Cursor
    {
        private final DeletionTime delTime = new DeletionTimeWrapper();
        private int idx;

        public ClusteringPrefix min()
        {
            return starts[idx];
        }

        public ClusteringPrefix max()
        {
            return ends[idx];
        }

        public DeletionTime delTime()
        {
            return delTime;
        }

        public void position(int i)
        {
            this.idx = i;
        }

        public int position()
        {
            return idx;
        }

        public int limit()
        {
            return size;
        }

        public ClusteringComparator comparator()
        {
            return comparator;
        }

        private class DeletionTimeWrapper extends DeletionTime
        {
            public long markedForDeleteAt()
            {
                return markedAts[idx];
            }

            public int localDeletionTime()
            {
                return delTimes[idx];
            }
        }
    }
}
