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

import com.google.common.base.Objects;

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

    public Layout metadata()
    {
        return data().metadata();
    }

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

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Row))
            return false;

        Row left = this;
        Row right = (Row)other;

        if (left.clusteringSize() != right.clusteringSize())
            return false;

        for (int i = 0; i < left.clusteringSize(); i++)
            if (!left.getClusteringColumn(i).equals(right.getClusteringColumn(i)))
                return false;

        if (left.cellsCount() != right.cellsCount())
            return false;

        int leftPos = left.startPosition();
        int rightPos = right.startPosition();

        int limit = left.endPosition();

        while (leftPos < limit)
        {
            if (!left.columnForPosition(leftPos).equals(right.columnForPosition(rightPos)))
                return false;

            boolean eq = left.isTombstone(leftPos) == right.isTombstone(rightPos)
                      && left.timestamp(leftPos) == right.timestamp(rightPos)
                      && left.ttl(leftPos) == right.ttl(rightPos)
                      && left.localDeletionTime(leftPos) == right.localDeletionTime(rightPos)
                      && Objects.equal(left.value(leftPos), right.value(rightPos))
                      && Objects.equal(left.key(leftPos), right.key(rightPos));

            if (!eq)
                return false;

            ++leftPos;
            ++rightPos;
        }
        return true;
    }

    /**
     * Convenient class to write/merge into an AbstractRow.
     */
    public static class Writer implements Rows.Writer
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

            int pos = r.startPosition();
            int limit = r.endPosition();
            while (pos < limit)
            {
                Column c = r.columnForPosition(pos);
                for (int i = 0; i < r.size(c); i++)
                {
                    addCell(c, r.isTombstone(pos), r.key(pos), r.value(pos), r.timestamp(pos), r.ttl(pos), r.localDeletionTime(pos));
                    ++pos;
                }
            }
            done();
        }

        public void addCell(Column c, boolean isTombstone, ByteBuffer key, ByteBuffer value, long timestamp, int ttl, long deletionTime)
        {
            int pos = helper.positionFor(c);

            if (isTombstone)
                data.setTombstone(pos);
            data.setKey(pos, key);
            data.setValue(pos, value);
            data.setTimestamp(pos, timestamp);
            data.setTTL(pos, ttl);
            data.setDeletionTime(pos, deletionTime);
        }

        public void done()
        {
            helper.done();
        }

        // Reset the writer to start writing at the beginning of the underlying RowData
        public void reset()
        {
            helper.reset();
        }
    }
}
