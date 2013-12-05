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
import java.util.*;

/**
 * Static utilities to work on Row objects.
 */
public abstract class Rows
{
    private Rows() {}

    public static void copyRow(Row row, Writer writer)
    {
        for (int i = 0; i < row.clusteringSize(); i++)
            writer.setClusteringColumn(i, row.getClusteringColumn(i));

        for (Cell cell : row)
            writer.addCell(cell);

        writer.done();
    }

    public static String toString(Atom atom)
    {
        return toString(atom, false);
    }

    public static String toString(Atom atom, boolean includeTimestamps)
    {
        if (atom == null)
            return "null";

        switch (atom.kind())
        {
            case ROW: return toString((Row)atom, includeTimestamps);
            case RANGE_TOMBSTONE: return toString((RangeTombstone)atom, includeTimestamps);
            case COLLECTION_TOMBSTONE: throw new UnsupportedOperationException(); // TODO
        }
        throw new AssertionError();
    }

    public static String toString(RangeTombstone rt, boolean includeTimestamps)
    {
        String str = String.format("[%s, %s]", toString(rt.metadata(), (Clusterable)rt.min()), toString(rt.metadata(), (Clusterable)rt.max()));
        if (includeTimestamps)
            str += "@" + rt.delTime().markedForDeleteAt();
        return str;
    }

    // TODO: not exactly at the right place
    public static String toString(Layout metadata, Clusterable c)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < c.clusteringSize(); i++)
        {
            if (i > 0) sb.append(":");
            sb.append(metadata.getClusteringType(i).getString((c.getClusteringColumn(i))));
        }
        return sb.toString();
    }

    public static String toString(Row row, boolean includeTimestamps)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(toString(row.metadata(), (Clusterable)row)).append("](");
        boolean isFirst = true;
        Column current = null;
        Iterator<Cell> iter = row.iterator();
        while (iter.hasNext())
        {
            if (isFirst) isFirst = false; else sb.append(", ");
            Cell cell = iter.next();
            sb.append(cell.column()).append(":");
            if (cell.column().isCollection())
            {
                int size = row.size(cell.column());
                sb.append("{");
                appendCell(sb, row.metadata(), cell, includeTimestamps);
                for (int i = 1; i < size; i++)
                    appendCell(sb.append(", "), row.metadata(), iter.next(), includeTimestamps);
                sb.append("}");
            }
            else
            {
                appendCell(sb, row.metadata(), cell, includeTimestamps);
            }
        }
        return sb.append(")").toString();
    }

    private static StringBuilder appendCell(StringBuilder sb, Layout metadata, Cell cell, boolean includeTimestamps)
    {
        if (cell.key() != null)
            sb.append(metadata.getKeyType(cell.column()).getString(cell.key())).append(":");
        sb.append(cell.value() == null ? "null" : metadata.getType(cell.column()).getString(cell.value()));
        if (includeTimestamps)
            sb.append("@").append(cell.timestamp());
        return sb;
    }

    // Merge multiple rows that are assumed to represent the same row (same clustering prefix).
    public static void merge(Layout layout, List<Row> rows, MergeHelper helper)
    {
        boolean isMerger = helper.writer instanceof Merger;

        if (isMerger)
        {
            assert rows.size() == 2;
            ((Merger)helper.writer).newMergedRows(rows.get(0), rows.get(1));
        }

        Row first = rows.get(0);
        for (int i = 0; i < layout.clusteringSize(); i++)
            helper.writer.setClusteringColumn(i, first.getClusteringColumn(i));

        helper.setRows(rows);
        Merger.Resolution resolution = null;
        while (helper.advance())
        {
            Column nextColumn = helper.nextColumn();
            int count = helper.nextMergedCount();
            Cell toMerge = null;
            for (int i = 0; i < count; i++)
            {
                Cell cell = helper.nextCell(i);
                if (cell == null)
                    continue;

                boolean overwite = toMerge == null || leftCellOverwrites(cell, toMerge, helper.now);

                // If we have a two row merger, sets the resolution
                if (isMerger)
                {
                    if (toMerge == null)
                        resolution = helper.isLeft(i) ? Merger.Resolution.ONLY_IN_LEFT : Merger.Resolution.ONLY_IN_RIGHT;
                    else
                        resolution = overwite ? Merger.Resolution.MERGED_FROM_RIGHT : Merger.Resolution.MERGED_FROM_LEFT;
                }

                if (overwite)
                    toMerge = cell;
            }
            addCell(toMerge, helper.writer, resolution);
        }
        helper.writer.done();
    }

    private static void addCell(Cell cell, Writer writer, Merger.Resolution resolution)
    {
        if (resolution != null)
            ((Merger)writer).nextCellResolution(resolution);
        writer.addCell(cell);
    }

    // TODO: deal with counters
    private static boolean leftCellOverwrites(Cell left, Cell right, int now)
    {
        if (left.timestamp() < right.timestamp())
            return false;
        if (left.timestamp() > right.timestamp())
            return true;

        // Tombstones take precedence (if the other is also a tombstone, it doesn't matter).
        if (left.isDeleted(now))
            return true;
        if (right.isDeleted(now))
            return true;

        // Same timestamp, no tombstones, compare values
        return left.value().compareTo(right.value()) < 0;
    }

    /**
     * Generic interface for a row writer.
     */
    public interface Writer
    {
        public void setClusteringColumn(int i, ByteBuffer value);
        public void addCell(Cell cell);
        public void addCell(Column c, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime);
        public void done();
    }

    /**
     * Handle writing the result of merging 2 rows.
     * <p>
     * Unlike a simple Writer, a Merger gets the information on where the resulting row comes from (the Resolution).
     */
    public interface Merger extends Writer
    {
        public enum Resolution { ONLY_IN_LEFT, ONLY_IN_RIGHT, MERGED_FROM_LEFT, MERGED_FROM_RIGHT }

        // Called before the merging of the 2 rows start to indicate which rows are merge being merged.
        public void newMergedRows(Row left, Row right);

        // Called before every addCell() call to indicate from which input row the newly added cell is from.
        public void nextCellResolution(Resolution resolution);
    }

    /**
     * Utility object to merge multiple rows.
     * <p>
     * We don't want to use a MergeIterator to merge multiple rows because we do that often
     * in the process of merging AtomIterators and we don't want to allocate iterators every
     * time (this object is reused over the course of merging multiple AtomIterator) and is
     * overall cheaper by being specialized.
     */
    public static class MergeHelper
    {
        public final Writer writer;
        public final int now;

        private List<Row> rows;

        private final Cell currentCells[];
        private final Iterator<Cell>[] iterators;

        private int nextMergedCount;
        private Column nextColumn;
        private final int[] nextIndexes;
        private final int[] nextRemainings;

        public MergeHelper(Writer writer, int now, int maxRowCount)
        {
            this.writer = writer;
            this.now = now;

            this.currentCells = new Cell[maxRowCount];
            this.iterators = (Iterator<Cell>[]) new Iterator[maxRowCount];
            this.nextIndexes = new int[maxRowCount];
            this.nextRemainings = new int[maxRowCount];
        }

        //@Override
        //public String toString()
        //{
        //    StringBuilder sb = new StringBuilder();
        //    sb.append("Rows:\n");
        //    for (int i = 0; i < rows.size(); i++)
        //        sb.append("  ").append(i).append(": ").append(toString(rows.get(i))).append("\n");
        //    sb.append("\ncurrent columns:").append(Arrays.toString(currentColumns));
        //    sb.append("\npositions:      ").append(Arrays.toString(positions));
        //    sb.append("\nlimits:         ").append(Arrays.toString(limits));
        //    sb.append("\n");
        //    sb.append("\nnext count:      ").append(nextMergedCount);
        //    sb.append("\nnext indexes:    ").append(Arrays.toString(nextIndexes));
        //    sb.append("\nnext remainings: ").append(Arrays.toString(nextRemainings));
        //    return sb.append("\n").toString();
        //}

        private void setRows(List<Row> rows)
        {
            this.rows = rows;

            nextMergedCount = 0;
            for (int i = 0; i < rows.size(); i++)
            {
                Iterator<Cell> iter = rows.get(i).iterator();
                iterators[i] = iter;
                currentCells[i] = iter.hasNext() ? iter.next() : null;
            }
        }

        // Find the next rows to merge together, setting nextMergedCount and
        // nextIndexes accordingly. Return false if we're done with the merge.
        private boolean advance()
        {
            boolean lastColumnDone = true;
            // First, advance on rows we've previously merged.
            for (int i = 0; i < nextMergedCount; i++)
            {
                // if we don't have remaining for that index, it means we're
                // still on a collection column but have no more element for
                // that specific row
                if (nextRemainings[i] == 0)
                    continue;

                int idx = nextIndexes[i];
                Row row = rows.get(idx);

                // nextColumn is the column we've just returned values for. If
                // it's the current column for the row considered, it means we've
                // just returned a value for it, advance the position.
                // Note: nextColumn can't be null since initially nextMergedCount == 0.
                if (nextColumn.equals(currentCells[idx].column()))
                {
                    currentCells[idx] = iterators[idx].hasNext() ? iterators[idx].next() : null;
                    if (--nextRemainings[i] != 0)
                        lastColumnDone = false;
                }
            }

            // If the last column is not done, we're all set.
            if (!lastColumnDone)
                return true;

            // Done with last column, find the next smallest column
            int nbRows = rows.size();
            nextColumn = null;
            for (int i = 0; i < nbRows; i++)
            {
                Column c = currentCells[i] == null ? null : currentCells[i].column();
                if (c != null && (nextColumn == null || nextColumn.compareTo(c) > 0))
                    nextColumn = c;
            }

            if (nextColumn == null)
                return false;

            // Lastly, collect the indexes/remainings of all row have this column
            nextMergedCount = 0;
            for (int i = 0; i < nbRows; i++)
            {
                Cell cell = currentCells[i];
                if (cell != null && cell.column().equals(nextColumn))
                {
                    nextIndexes[nextMergedCount] = i;
                    nextRemainings[nextMergedCount] = rows.get(i).size(cell.column());
                    ++nextMergedCount;
                }
            }
            return true;
        }

        private Column nextColumn()
        {
            return nextColumn;
        }

        private int nextMergedCount()
        {
            return nextMergedCount;
        }

        private Cell nextCell(int i)
        {
            return nextRemainings[i] == 0 ? null : currentCells[nextIndexes[i]];
        }

        private boolean isLeft(int i)
        {
            // This should only be called if we have only 2 rows.
            return nextIndexes[i] == 0;
        }
    }
}
