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

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
    private Rows() {}

    /**
     * Merge two rows, assuming they do represent the same logical row (i.e.
     * that they share the same clustering prefix).
     */
    public static void merge(Layout layout, Row left, Row right, Rows.Merger merger, int now)
    {
        for (int i = 0; i < layout.clusteringSize(); i++)
            merger.setClusteringColumn(i, left.getClusteringColumn(i));

        int leftPos = left.startPosition();
        int rightPos = right.startPosition();
        int leftLimit = leftPos + left.cellsCount();
        int rightLimit = rightPos + right.cellsCount();
        Column leftColumn = left.columnForPosition(leftPos);
        Column rightColumn = right.columnForPosition(rightPos);

        while (leftColumn != null || rightColumn != null)
        {
            int cmp = leftColumn == null ? 1 : (rightColumn == null ? - 1 : leftColumn.compareTo(rightColumn));
            if (cmp < 0)
            {
                leftPos += addCells(leftColumn, left, leftPos, merger, Rows.Merger.Resolution.ONLY_IN_LEFT);
                leftColumn = leftPos < leftLimit ? left.columnForPosition(leftPos) : null;
            }
            else if (cmp > 0)
            {
                rightPos += addCells(rightColumn, right, rightPos, merger, Rows.Merger.Resolution.ONLY_IN_RIGHT);
                rightColumn = rightPos < rightLimit ? right.columnForPosition(rightPos) : null;
            }
            else
            {
                Column c = leftColumn;
                if (c.isCollection())
                {
                    Comparator<ByteBuffer> keyComparator = layout.collectionKeyComparator(c);
                    int leftCollectionLimit = leftPos + left.size(c);
                    int rightCollectionLimit = rightPos + right.size(c);
                    ByteBuffer leftKey = left.key(leftPos);
                    ByteBuffer rightKey = right.key(rightPos);
                    while (leftPos < leftCollectionLimit || rightPos < rightCollectionLimit)
                    {
                        cmp = leftKey == null ? 1 : (rightKey == null ? -1 : keyComparator.compare(leftKey, rightKey));
                        if (cmp < 0)
                        {
                            addCell(c, left, leftPos++, merger, Rows.Merger.Resolution.ONLY_IN_LEFT);
                            leftKey = leftPos < leftCollectionLimit ? left.key(leftPos) : null;
                        }
                        else if (cmp > 0)
                        {
                            addCell(c, right, rightPos++, merger, Rows.Merger.Resolution.ONLY_IN_RIGHT);
                            rightKey = rightPos < rightCollectionLimit ? right.key(rightPos) : null;
                        }
                        else
                        {
                            merge(c, left, leftPos++, right, rightPos++, merger, now);
                            leftKey = leftPos < leftCollectionLimit ? left.key(leftPos) : null;
                            rightKey = rightPos < rightCollectionLimit ? right.key(rightPos) : null;
                        }
                    }
                }
                else
                {
                    merge(c, left, leftPos++, right, rightPos++, merger, now);
                }
                leftColumn = leftPos < leftLimit ? left.columnForPosition(leftPos) : null;
                rightColumn = rightPos < rightLimit ? right.columnForPosition(rightPos) : null;
            }
        }
        merger.rowDone();
    }

    private static int addCells(Column c, Row row, int pos, Merger merger, Merger.Resolution resolution)
    {
        if (c.isCollection())
        {
            int size = row.size(c);
            int limit = pos + size;
            for (int i = pos; i < limit; i++)
                addCell(c, row, i, merger, resolution);
            return size;
        }
        else
        {
            addCell(c, row, pos, merger, resolution);
            return 1;
        }
    }

    private static void addCell(Column c, Row row, int pos, Merger merger, Merger.Resolution resolution)
    {
        merger.addCell(c, resolution, row.isTombstone(pos), row.key(pos), row.value(pos), row.timestamp(pos), row.ttl(pos), row.localDeletionTime(pos));
    }

    private static void merge(Column c, Row left, int leftPos, Row right, int rightPos, Merger merger, int now)
    {
        if (leftCellOverwrites(left, leftPos, right, rightPos, now))
            addCell(c, left, leftPos, merger, Rows.Merger.Resolution.MERGED_FROM_LEFT);
        else
            addCell(c, right, rightPos, merger, Rows.Merger.Resolution.MERGED_FROM_RIGHT);
    }

    // TODO: deal with counters
    private static boolean leftCellOverwrites(Row left, int leftPos, Row right, int rightPos, int now)
    {
        if (left.timestamp(leftPos) < right.timestamp(rightPos))
            return false;
        if (left.timestamp(leftPos) > right.timestamp(rightPos))
            return true;

        // Tombstones take precedence (if the other is also a tombstone, it doesn't matter).
        if (left.isDeleted(leftPos, now))
            return true;
        if (right.isDeleted(rightPos, now))
            return true;

        // Same timestamp, no tombstones, compare values
        return left.value(leftPos).compareTo(right.value(rightPos)) < 0;
    }

    public interface Merger
    {
        public enum Resolution { ONLY_IN_LEFT, ONLY_IN_RIGHT, MERGED_FROM_LEFT, MERGED_FROM_RIGHT }

        public void setClusteringColumn(int i, ByteBuffer value);

        public void addCell(Column c, Resolution resolution, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime);

        // Called when the row is fully merged
        public void rowDone();
    }
}
