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

    public Atom.Kind kind()
    {
        return Atom.Kind.ROW;
    }

    protected abstract int row();
    protected abstract RowData data();

    public int clusteringSize()
    {
        return data().clusteringSize();
    }

    public ByteBuffer getClusteringColumn(int i)
    {
        return data().clusteringColumn(row(), i);
    }

    public int startPosition()
    {
        return data().startPosition(row());
    }

    public int endPosition()
    {
        return data().endPosition(row());
    }

    public int cellsCount()
    {
        return data().cellsCount(row());
    }

    public boolean has(Column c)
    {
        return position(c) >= 0;
    }

    public int position(Column c)
    {
        return data().position(row(), c);
    }

    public int size(Column c)
    {
        return data().size(row(), c);
    }

    public Column columnForPosition(int pos)
    {
        return data().columnForPosition(row(), pos);
    }

    public boolean isDeleted(int pos, int now)
    {
        return isTombstone(pos) || localDeletionTime(pos) <= now;
    }

    public boolean isTombstone(int pos)
    {
        return data().isTombstone(pos);
    }

    public ByteBuffer value(int pos)
    {
        return data().value(pos);
    }

    public long timestamp(int pos)
    {
        return data().timestamp(pos);
    }

    public int ttl(int pos)
    {
        return data().ttl(pos);
    }

    public long localDeletionTime(int pos)
    {
        return data().deletionTime(pos);
    }

    public ByteBuffer key(int pos)
    {
        return data().key(pos);
    }

    /**
     * Convenient class to write/merge into an AbstractRow.
     */
    public static class Writer implements Rows.Merger
    {
        private final RowData data;
        private final RowData.WriteHelper helper;

        protected Writer(RowData data)
        {
            this.data = data;
            this.helper = data.createWriteHelper();
        }

        public void setClusteringColumn(int i, ByteBuffer value)
        {
            data.setClusteringColumn(helper.row(), i, value);
        }

        public void copyRow(Row r)
        {
            for (int i = 0; i < r.clusteringSize(); i++)
                setClusteringColumn(i, r.getClusteringColumn(i));

            for (int pos = r.startPosition(); pos < r.endPosition(); pos++)
            {
                Column c = r.columnForPosition(pos);
                for (int i = 0; i < r.size(c); i++)
                {
                    addCell(c, r.isTombstone(pos), r.key(pos), r.value(pos), r.timestamp(pos), r.ttl(pos), r.localDeletionTime(pos));
                    ++pos;
                }
            }
            rowDone();
        }

        public void addCell(Column c, Rows.Merger.Resolution resolution, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime)
        {
            // Ignore the resolution
            addCell(c, isTombstone, key, value, timestamp, ttl, deletionTime);
        }

        public void addCell(Column c, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime)
        {
            int pos = helper.positionFor(c);

            if (isTombstone)
                data.setTombstone(pos);
            if (c.isCollection())
                data.setKey(pos, key);
            data.setValue(pos, value);
            data.setTimestamp(pos, timestamp);
            data.setTTL(pos, ttl);
            data.setDeletionTime(pos, deletionTime);
        }

        public void rowDone()
        {
            helper.rowDone();
        }

        // Reset the writer to start writing at the beginning of the underlying RowData
        public void reset()
        {
            helper.reset();
        }
    }
}
