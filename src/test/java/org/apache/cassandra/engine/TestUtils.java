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

/**
 * Static helper methods for tests.
 */
public abstract class TestUtils
{
    private TestUtils() {}

    public static Column col(String name)
    {
        return new Column(name, false);
    }

    public static Column ccol(String name)
    {
        return new Column(name, true);
    }

    public static int i(ByteBuffer bb)
    {
        return bb.getInt(0);
    }

    public static ByteBuffer bb(int value)
    {
        return ByteBuffer.allocate(4).putInt(0, value);
    }

    public static RowWriter writeTo(AbstractRow row)
    {
        return new RowWriter(row);
    }

    /**
     * Convenient wrapper over AbstractRow.Writer for easier testing.
     */
    public static class RowWriter
    {
        private final AbstractRow.Writer writer;

        private RowWriter(AbstractRow row)
        {
            this.writer = row.writer();
        }

        public RowWriter clustering(int... values)
        {
            for (int i = 0; i < values.length; i++)
                writer.setClusteringColumn(i, bb(values[i]));
            return this;
        }

        public RowWriter add(String name, int value, long timestamp)
        {
            return add(name, value, timestamp, 0);
        }

        public RowWriter add(String name, int value, long timestamp, int ttl)
        {
            writer.addCell(col(name), false, bb(value), timestamp, ttl, ttl == 0 ? AbstractRow.NO_LOCAL_DELETION_TIME : System.currentTimeMillis() + (ttl * 1000));
            return this;
        }

        public RowWriter addTombstone(String name, long timestamp)
        {
            writer.addCell(col(name), true, null, timestamp, 0, System.currentTimeMillis());
            return this;
        }

        public RowWriter add(String name, int key, int value, long timestamp)
        {
            writer.addCollectionCell(ccol(name), bb(key), false, bb(value), timestamp, 0, AbstractRow.NO_LOCAL_DELETION_TIME);
            return this;
        }

        public RowWriter addTombstone(String name, int key, long timestamp)
        {
            writer.addCollectionCell(ccol(name), bb(key), true, null, timestamp, 0, System.currentTimeMillis());
            return this;
        }

        public void done()
        {
            writer.closeRow();
        }
    }
}
