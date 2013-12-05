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

import static org.junit.Assert.*;

/**
 * Static helper methods for tests.
 */
public abstract class TestUtils
{
    private TestUtils() {}

    private static final Layout simpleLayout = new Layout()
    {
        private final Column[] columns = new Column[] { col("a"), col("b"), ccol("c1"), ccol("c2"), col("z") };
        private final ClusteringComparator comparator = new AbstractClusteringComparator()
        {
            public int compare(Clusterable c1, Clusterable c2)
            {
                if (c1.clusteringSize() == 0)
                    return c2.clusteringSize() == 0 ? 0 : -1;

                if (c2.clusteringSize() == 0)
                    return 1;

                return i(c1.getClusteringColumn(0)) - i(c2.getClusteringColumn(0));
            }
        };
        private final Layout.Type type = new Layout.Type()
        {
            public String getString(ByteBuffer buffer) { return String.valueOf(i(buffer)); }
        };

        public int clusteringSize() { return 1; }
        public Column[] regularColumns() { return columns; }
        public ClusteringComparator comparator() { return comparator; }
        public boolean hasCollections() { return true; }
        public Comparator<ByteBuffer> collectionKeyComparator(Column c)
        {
            return new Comparator<ByteBuffer>()
            {
                public int compare(ByteBuffer b1, ByteBuffer b2)
                {
                    return i(b1) - i(b2);
                }
            };
        }
        public Type getClusteringType(int i) { return type; }
        public Type getKeyType(Column c) {return type; }
        public Type getType(Column c) {return type; }

    };

    /**
     * Returns a Layout correspond to:
     *   CREATE TABLE test (
     *       pk int,
     *       cc int,
     *       a int,
     *       b int,
     *       c1 map<int, int>
     *       c2 map<int, int>
     *       z int,
     *       PRIMARY KEY (pk, cc)
     *   )
     */
    public static Layout simpleLayout()
    {
        return simpleLayout;
    }

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

    public static int ival(Row row, String name)
    {
        return i(row.get(col(name)).value());
    }

    public static int ival(Row row, String name, int i)
    {
        return i(row.get(col(name), i).value());
    }

    public static long tstamp(Row row, String name)
    {
        return row.get(col(name)).timestamp();
    }

    public static long tstamp(Row row, String name, int i)
    {
        return row.get(col(name), i).timestamp();
    }

    public static ByteBuffer bb(int value)
    {
        return ByteBuffer.allocate(4).putInt(0, value);
    }

    public static RowPath path(int value)
    {
        return new RowPath(bb(value));
    }

    public static RangeTombstone rt(final int min, final int max, final long tstamp)
    {
        return new RangeTombstone()
        {
            private final ClusteringPrefix pmin = new ClusteringPrefix(bb(min));
            private final ClusteringPrefix pmax = new ClusteringPrefix(bb(max));
            private final DeletionTime delTime = DeletionTime.createImmutable(tstamp, (int)(System.currentTimeMillis() / 1000));

            public Layout metadata() { return simpleLayout; }
            public ClusteringPrefix min() { return pmin; }
            public ClusteringPrefix max() { return pmax; }
            public DeletionTime delTime() { return delTime; }
        };
    }

    public static DecoratedKey dk(int i)
    {
        return new IntDecoratedKey(i);
    }

    public static Slices slices(int start, int end)
    {
        return Slices.create(new ClusteringPrefix(bb(start)), new ClusteringPrefix(bb(end)));
    }

    public static RowWriter writeTo(ReusableRow row)
    {
        return new RowWriter(row);
    }

    public static RowWriter newRowWriter(Layout layout)
    {
        return new RowWriter(new ReusableRow(layout, 4));
    }

    public static AtomIteratorBuilder newAtomIteratorBuilder(Layout layout, DecoratedKey partitionKey)
    {
        return new AtomIteratorBuilder(layout, partitionKey);
    }

    public static void assertSameIterator(AtomIterator expected, AtomIterator actual)
    {
        assertEquals(expected.getPartitionKey(), actual.getPartitionKey());
        assertEquals(expected.partitionLevelDeletion(), actual.partitionLevelDeletion());

        while (expected.hasNext())
        {
            assertTrue(actual.hasNext());
            Atom e = expected.next();
            Atom a = actual.next();
            assertEquals(Rows.toString(e, true) + " != " + Rows.toString(a, true), e, a);
        }
    }

    /**
     * Convenient wrapper over AbstractRow.Writer for easier testing.
     */
    public static class RowWriter
    {
        private final ReusableRow row;
        private final Rows.Writer writer;

        private RowWriter(ReusableRow row)
        {
            this.row = row;
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
            writer.addCell(col(name), false, null, bb(value), timestamp, ttl, ttl == 0 ? AbstractRow.NO_LOCAL_DELETION_TIME : System.currentTimeMillis() + (ttl * 1000));
            return this;
        }

        public RowWriter addTombstone(String name, long timestamp)
        {
            writer.addCell(col(name), true, null, null, timestamp, 0, System.currentTimeMillis());
            return this;
        }

        public RowWriter add(String name, int key, int value, long timestamp)
        {
            writer.addCell(ccol(name), false, bb(key), bb(value), timestamp, 0, AbstractRow.NO_LOCAL_DELETION_TIME);
            return this;
        }

        public RowWriter addTombstone(String name, int key, long timestamp)
        {
            writer.addCell(ccol(name), true, bb(key), null, timestamp, 0, System.currentTimeMillis());
            return this;
        }

        public ReusableRow done()
        {
            writer.done();
            row.reset();
            return row;
        }
    }

    public static class AtomIteratorBuilder
    {
        private final Layout layout;
        private final DecoratedKey pk;
        private DeletionTime delTime = DeletionTime.LIVE;
        private final List<Atom> atoms = new ArrayList<Atom>();

        private AtomIteratorBuilder(Layout layout, DecoratedKey partitionKey)
        {
            this.layout = layout;
            this.pk = partitionKey;
        }

        public AtomIteratorBuilder topLevelDeletion(DeletionTime delTime)
        {
            this.delTime = delTime;
            return this;
        }

        public AtomIteratorBuilder add(Atom atom)
        {
            if (!atoms.isEmpty() && layout.comparator().atomComparator().compare(atom, atoms.get(atoms.size() - 1)) <= 0)
                throw new IllegalArgumentException("Added out of order atom");
            atoms.add(atom);
            return this;
        }

        public AtomIterator build()
        {
            return new AtomIterator()
            {
                private int idx = 0;

                public Layout metadata()
                {
                    return layout;
                }

                public DecoratedKey getPartitionKey()
                {
                    return pk;
                }

                public DeletionTime partitionLevelDeletion()
                {
                    return delTime;
                }

                public boolean hasNext()
                {
                    return idx < atoms.size();
                }

                public Atom next()
                {
                    return atoms.get(idx++);
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }

                public void close()
                {
                }
            };
        }
    }

    private static class IntDecoratedKey implements DecoratedKey
    {
        private final int val;

        public IntDecoratedKey(int val)
        {
            this.val = val;
        }

        @Override
        public String toString()
        {
            return String.valueOf(val);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof IntDecoratedKey))
                return false;
            return val == ((IntDecoratedKey)other).val;
        }

        @Override
        public int hashCode()
        {
            return new Integer(val).hashCode();
        }
    }
}
