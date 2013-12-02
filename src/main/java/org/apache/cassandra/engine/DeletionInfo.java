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

import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import org.apache.cassandra.engine.utils.CursorBasedIterator;

public class DeletionInfo
{
    // We don't have way to represent the full interval of keys (Interval don't support the minimum token as the right bound),
    // so we keep the topLevel deletion info separatly. This also slightly optimize the case of full row deletion which is rather common.
    private DeletionTime topLevel;
    private RangeTombstoneList ranges; // null if no range tombstones (to save an allocation since it's a common case).

    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(DeletionTime.createImmutable(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Integer.MAX_VALUE : localDeletionTime));
    }

    public DeletionInfo(DeletionTime topLevel)
    {
        this(topLevel, null);
    }

    public DeletionInfo(ClusteringPrefix start, ClusteringPrefix end, ClusteringComparator comparator, long markedForDeleteAt, int localDeletionTime)
    {
        this(DeletionTime.LIVE, new RangeTombstoneList(comparator, 1));
        ranges.add(start, end, markedForDeleteAt, localDeletionTime);
    }

    public DeletionInfo(RangeTombstone rangeTombstone, ClusteringComparator comparator)
    {
        this(rangeTombstone.min(), rangeTombstone.max(), comparator, rangeTombstone.delTime().markedForDeleteAt(), rangeTombstone.delTime().localDeletionTime());
    }

    public static DeletionInfo live()
    {
        return new DeletionInfo(DeletionTime.LIVE);
    }

    private DeletionInfo(DeletionTime topLevel, RangeTombstoneList ranges)
    {
        this.topLevel = topLevel;
        this.ranges = ranges;
    }

    public DeletionInfo copy()
    {
        return new DeletionInfo(topLevel, ranges == null ? null : ranges.copy());
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return !hasTopLevelDeletion()
            && (ranges == null || ranges.isEmpty());
    }

    public boolean isDeleted(Clusterable name, long timestamp)
    {
        // We do rely on this test: if topLevel.markedForDeleteAt is MIN_VALUE, we should not
        // consider the column deleted even if timestamp=MIN_VALUE, otherwise this break QueryFilter.isRelevant
        if (isLive())
            return false;

        if (timestamp <= topLevel.markedForDeleteAt())
            return true;

        return ranges != null && ranges.isDeleted(name, timestamp);
    }

    /**
     * Returns a new {@link InOrderTester} in forward order.
     */
    InOrderTester inOrderTester()
    {
        return inOrderTester(false);
    }

    /**
     * Returns a new {@link InOrderTester} given the order in which
     * columns will be passed to it.
     */
    public InOrderTester inOrderTester(boolean reversed)
    {
        return new InOrderTester(reversed);
    }

    /**
     * Purge every tombstones that are older than {@code gcbefore}.
     *
     * @param gcBefore timestamp (in seconds) before which tombstones should
     * be purged
     */
    public void purge(int gcBefore)
    {
        topLevel = topLevel.localDeletionTime() < gcBefore ? DeletionTime.LIVE : topLevel;

        if (ranges != null)
        {
            ranges.purge(gcBefore);
            if (ranges.isEmpty())
                ranges = null;
        }
    }

    public boolean hasIrrelevantData(int gcBefore)
    {
        if (topLevel.localDeletionTime() < gcBefore)
            return true;

        return ranges != null && ranges.hasIrrelevantData(gcBefore);
    }

    public void add(DeletionTime newInfo)
    {
        if (topLevel.markedForDeleteAt() < newInfo.markedForDeleteAt())
            topLevel = newInfo;
    }

    public void add(RangeTombstone tombstone, ClusteringComparator comparator)
    {
        if (ranges == null)
            ranges = new RangeTombstoneList(comparator, 1);

        ranges.add(tombstone);
    }

    /**
     * Adds the provided deletion infos to the current ones.
     *
     * @return this object.
     */
    public DeletionInfo add(DeletionInfo newInfo)
    {
        add(newInfo.topLevel);

        if (ranges == null)
            ranges = newInfo.ranges == null ? null : newInfo.ranges.copy();
        else if (newInfo.ranges != null)
            ranges.addAll(newInfo.ranges);

        return this;
    }

    public long minTimestamp()
    {
        return ranges == null
             ? topLevel.markedForDeleteAt()
             : Math.min(topLevel.markedForDeleteAt(), ranges.minMarkedAt());
    }

    /**
     * The maximum timestamp mentioned by this DeletionInfo.
     */
    public long maxTimestamp()
    {
        return ranges == null
             ? topLevel.markedForDeleteAt()
             : Math.max(topLevel.markedForDeleteAt(), ranges.maxMarkedAt());
    }

    public DeletionTime getTopLevelDeletion()
    {
        return topLevel;
    }

    public boolean hasTopLevelDeletion()
    {
        return !topLevel.isLive();
    }

    public DeletionTime rangeCovering(ClusteringPrefix name)
    {
        return ranges == null ? null : ranges.search(name);
    }

    public boolean hasRanges()
    {
        return ranges != null && !ranges.isEmpty();
    }

    public CursorBasedIterator<RangeTombstoneList.Cursor> rangeIterator()
    {
        return ranges == null ? CursorBasedIterator.<RangeTombstoneList.Cursor>emptyIterator() : ranges.iterator();
    }

    //@Override
    //public String toString()
    //{
    //    if (ranges == null || ranges.isEmpty())
    //        return String.format("{%s}", topLevel);
    //    else
    //        return String.format("{%s, ranges=%s}", topLevel, rangesAsString());
    //}

    //private String rangesAsString()
    //{
    //    assert !ranges.isEmpty();
    //    StringBuilder sb = new StringBuilder();
    //    CType type = (CType)ranges.comparator();
    //    assert type != null;
    //    Iterator<RangeTombstone> iter = rangeIterator();
    //    while (iter.hasNext())
    //    {
    //        RangeTombstone i = iter.next();
    //        sb.append("[");
    //        sb.append(type.getString(i.min())).append("-");
    //        sb.append(type.getString(i.max())).append(", ");
    //        sb.append(i.data);
    //        sb.append("]");
    //    }
    //    return sb.toString();
    //}

    // Updates all the timestamp of the deletion contained in this DeletionInfo to be {@code timestamp}.
    public void updateAllTimestamp(long timestamp)
    {
        if (topLevel.markedForDeleteAt() != Long.MIN_VALUE)
            topLevel = DeletionTime.createImmutable(timestamp, topLevel.localDeletionTime());

        if (ranges != null)
            ranges.updateAllTimestamp(timestamp);
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionInfo))
            return false;
        DeletionInfo that = (DeletionInfo)o;
        return topLevel.equals(that.topLevel) && Objects.equal(ranges, that.ranges);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(topLevel, ranges);
    }

    /**
     * This object allow testing whether a given column (name/timestamp) is deleted
     * or not by this DeletionInfo, assuming that the column given to this
     * object are passed in forward or reversed comparator sorted order.
     *
     * This is more efficient that calling DeletionInfo.isDeleted() repeatedly
     * in that case.
     */
    public class InOrderTester
    {
        /*
         * Note that because because range tombstone are added to this DeletionInfo while we iterate,
         * ranges may be null initially and we need to wait the first range to create the tester (once
         * created the test will pick up new tombstones however). We do are guaranteed that a range tombstone
         * will be added *before* we test any column that it may delete so this is ok.
         */
        private RangeTombstoneList.InOrderTester tester;
        private final boolean reversed;

        private InOrderTester(boolean reversed)
        {
            this.reversed = reversed;
        }

        public boolean isDeleted(Clusterable name, long timestamp)
        {
            if (timestamp <= topLevel.markedForDeleteAt())
                return true;

            /*
             * We don't optimize the reversed case for now because RangeTombstoneList
             * is always in forward sorted order.
             */
            if (reversed)
                 return DeletionInfo.this.isDeleted(name, timestamp);

            // Maybe create the tester if we hadn't yet and we now have some ranges (see above).
            if (tester == null && ranges != null)
                tester = ranges.inOrderTester();

            return tester != null && tester.isDeleted(name, timestamp);
        }
    }
}
