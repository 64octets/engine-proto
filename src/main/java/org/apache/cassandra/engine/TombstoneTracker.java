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

import java.util.Arrays;

/**
 * Utility object that expect rows and tombstones in comparator order
 * and track which tombstones are currently open.
 */
public class TombstoneTracker
{
    // In a number of cases, this will be feeded by the tombsone of a RangeTombstoneList. In this
    // case, we'll only care about one tombstone at a time. When we merge multiple iterators however,
    // it might be that multiple tombstone will be open at the same time and we need to handle.
    // However, even when that is the case, we know we can't have a lot of range tombstones open at the
    // same time and this class use that fact to keep things simple.
    private final ClusteringComparator comparator;

    // Holds the end prefix for opened range tombstone with their corresponding timestamp. The timestamp
    // is always the biggest timestamp until the corresponding end so when testing for deletions we only
    // even need to check until the end is greater than the currently checked value.
    private int size;
    private ClusteringPrefix[] ends;
    private long[] timestamps;

    public TombstoneTracker(ClusteringComparator comparator)
    {
        this.comparator = comparator;
        // As said above, the case of size 1 is not uncommon, so start there and we'll grow if need be
        this.ends = new ClusteringPrefix[1];
        this.timestamps = new long[1];
    }

    public void update(Atom atom)
    {
        switch (atom.kind())
        {
            case ROW:
                discardUntil((Row)atom);
                break;
            case RANGE_TOMBSTONE:
                updateForRangeTombstone((RangeTombstone)atom);
                break;
            case COLLECTION_TOMBSTONE:
                throw new AssertionError();
        }
    }

    private void discardUntil(Clusterable c)
    {
        int toDiscard = 0;
        while (toDiscard < size && comparator.compare(c, ends[toDiscard]) > 1)
            toDiscard++;

        if (toDiscard >= size)
            size = 0;
        else
            copy(toDiscard, 0, size - toDiscard);
    }

    private void updateForRangeTombstone(RangeTombstone rt)
    {
        discardUntil(rt.min());

        ClusteringPrefix end = rt.max();
        long timestamp = rt.delTime().markedForDeleteAt();

        for (int i = 0; i < size; i++)
        {
            int cmp = comparator.compare(end, ends[i]);
            if (cmp < 0)
            {
                // new range ends before the current one. If the current timestamp is greater
                // than the new one, we don't care about that new range and we're done. Otherwise
                // we need to "insert" the new end with it's timestamp (and we're done too).
                if (timestamp > timestamps[i])
                    insert(i, end, timestamp);
                return;
            }
            else
            {
                // new range ends after the current one. Keep the biggest timestamp until the
                // current end and continue (unless the ends where actually the same and we're done)
                timestamps[i] = Math.max(timestamps[i], timestamp);
                if (cmp == 0)
                    return;
            }
        }
        // If we got there, we've reached the end but sill have to add the new tombstone
        add(end, timestamp);
    }

    private void add(ClusteringPrefix prefix, long timestamp)
    {
        maybeGrow();
        ends[size] = prefix;
        timestamps[size] = timestamp;
        ++size;
    }

    private void insert(int i, ClusteringPrefix prefix, long timestamp)
    {
        maybeGrow();
        copy(i, i+1, size - i);
        ends[i] = prefix;
        timestamps[i] = timestamp;
        ++size;
    }

    private void maybeGrow()
    {
        if (size + 1 < ends.length)
            return;

        int newSize = ends.length == 1 ? 5 : (3 * ends.length / 2) + 1;
        ends = Arrays.copyOf(ends, newSize);
        timestamps = Arrays.copyOf(timestamps, newSize);
    }

    private void copy(int start, int end, int length)
    {
        System.arraycopy(ends, start, ends, end, length);
        System.arraycopy(timestamps, start, timestamps, end, length);
    }

    public boolean isDeleted(Clusterable c, long timestamp)
    {
        for (int i = 0; i < size; i++)
        {
            if (comparator.compare(c, ends[i]) > 0)
                return false;

            if (timestamps[i] >= timestamp)
                return true;
        }
        return false;
    }
}
