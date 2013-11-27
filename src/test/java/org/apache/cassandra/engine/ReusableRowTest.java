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

import org.junit.Test;
import static org.junit.Assert.*;

import static org.apache.cassandra.engine.TestUtils.*;

public class ReusableRowTest
{
    private static final Layout layout = new Layout()
    {
        private Column[] columns = new Column[] {
            // regular
            col("a"), col("b"), col("c"), col("d"),
            // collections
            ccol("m"), ccol("n"), ccol("o"),
            // regular
            col("x"), col("y"), col("z"),
        };

        public int clusteringSize() { return 0; }
        public Column[] regularColumns() { return columns; }
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
    };

    @Test
    public void testReusability()
    {
        ReusableRow row = new ReusableRow(layout, 4, true);
        writeTo(row).add("a", 2, 0L)
                    .add("c", 4, 0L)
                    .add("m", 0, 0, 0L)
                    .add("m", 1, 1, 0L)
                    .done();

        assertTrue(row.has(col("a")));
        assertTrue(row.has(col("c")));
        assertTrue(row.has(ccol("m")));

        assertFalse(row.has(col("b")));
        assertFalse(row.has(col("x")));

        assertEquals(4, i(row.value(col("c"))));
        assertEquals(1, i(row.value(ccol("m"), 1)));

        assertEquals(2, row.size(ccol("m")));
        assertEquals(0, row.size(ccol("n")));

        // Reuse
        writeTo(row).add("b", 3, 0L)
                    .add("c", 6, 0L)
                    .add("m", 0, 5, 0L)
                    .add("x", 1, 0L)
                    .add("y", 3, 0L)
                    .done();

        assertTrue(row.has(col("b")));
        assertTrue(row.has(col("c")));
        assertTrue(row.has(ccol("m")));
        assertTrue(row.has(col("x")));
        assertTrue(row.has(col("y")));

        assertFalse(row.has(col("a")));
        assertFalse(row.has(col("d")));
        assertFalse(row.has(ccol("n")));

        assertEquals(6, i(row.value(col("c"))));
        assertEquals(5, i(row.value(ccol("m"), 0)));
        assertEquals(3, i(row.value(col("y"), 0)));

        assertEquals(1, row.size(ccol("m")));
        assertEquals(0, row.size(ccol("n")));
    }

    @Test
    public void testMerging()
    {
        ReusableRow left = new ReusableRow(layout, 3, true);
        writeTo(left).add("a", 2, 0L)
                     .add("b", 4, 0L)
                     .add("m", 0, 0, 1L)
                     .add("m", 1, 0, 1L)
                     .done();

        ReusableRow right = new ReusableRow(layout, 3, true);
        writeTo(right).add("a", 3, 1L)
                      .add("m", 0, 1, 0L)
                      .add("m", 1, 1, 1L)
                      .add("m", 2, 1, 2L)
                      .done();

        ReusableRow result = new ReusableRow(layout, 2, true);
        AbstractRow.merge(layout, left, right, result.writer(), 0);

        assertTrue(result.has(col("a")));
        assertTrue(result.has(col("b")));
        assertTrue(result.has(ccol("m")));

        assertFalse(result.has(col("c")));
        assertFalse(result.has(ccol("n")));

        assertEquals(3, result.size(ccol("m")));

        assertEquals(3, i(result.value(col("a"))));
        assertEquals(4, i(result.value(col("b"))));
        assertEquals(0, i(result.value(ccol("m"), 0)));
        assertEquals(0, i(result.value(ccol("m"), 1)));
        assertEquals(1, i(result.value(ccol("m"), 2)));

        assertEquals(1L, result.timestamp(col("a")));
        assertEquals(0L, result.timestamp(col("b")));
        assertEquals(1L, result.timestamp(ccol("m"), 0));
        assertEquals(1L, result.timestamp(ccol("m"), 1));
        assertEquals(2L, result.timestamp(ccol("m"), 2));
    }
}
