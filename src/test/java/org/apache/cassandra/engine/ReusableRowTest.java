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
    @Test
    public void testReusability()
    {
        Layout layout = simpleLayout();
        ReusableRow row = new ReusableRow(layout, 4);
        writeTo(row).clustering(0)
                    .add("a", 2, 0L)
                    .add("b", 4, 0L)
                    .add("c1", 0, 0, 0L)
                    .add("c1", 1, 1, 0L)
                    .done();

        assertEquals(1, row.clusteringSize());
        assertEquals(0, i(row.getClusteringColumn(0)));

        assertTrue(row.has(col("a")));
        assertTrue(row.has(col("b")));
        assertTrue(row.has(ccol("c1")));

        assertFalse(row.has(col("z")));
        assertFalse(row.has(col("c2")));

        assertEquals(4, ival(row, "b"));
        assertEquals(1, ival(row, "c1", 1));

        assertEquals(2, row.size(ccol("c1")));
        assertEquals(0, row.size(ccol("c2")));

        // Reuse
        writeTo(row).clustering(1)
                    .add("b", 1, 0L)
                    .add("c1", 0, 5, 0L)
                    .add("c2", 0, 1, 0L)
                    .add("z", 3, 0L)
                    .done();

        assertEquals(1, row.clusteringSize());
        assertEquals(1, i(row.getClusteringColumn(0)));

        assertTrue(row.has(col("b")));
        assertTrue(row.has(ccol("c1")));
        assertTrue(row.has(ccol("c2")));
        assertTrue(row.has(col("z")));

        assertFalse(row.has(col("a")));

        assertEquals(1, ival(row, "b"));
        assertEquals(5, ival(row, "c1", 0));
        assertEquals(1, ival(row, "c2", 0));
        assertEquals(3, ival(row, "z"));

        assertEquals(1, row.size(ccol("c1")));
        assertEquals(1, row.size(ccol("c2")));
    }

    @Test
    public void testMerging()
    {
        Layout layout = simpleLayout();
        ReusableRow left = new ReusableRow(layout, 3);
        writeTo(left).clustering(0)
                     .add("a", 2, 0L)
                     .add("b", 4, 0L)
                     .add("c1", 0, 0, 1L)
                     .add("c1", 1, 0, 1L)
                     .done();

        ReusableRow right = new ReusableRow(layout, 3);
        writeTo(right).clustering(0)
                      .add("a", 3, 1L)
                      .add("c1", 0, 1, 0L)
                      .add("c1", 1, 1, 1L)
                      .add("c1", 2, 1, 2L)
                      .done();

        ReusableRow result = new ReusableRow(layout, 2);
        Rows.merge(layout, left, right, result.writer(), 0);

        assertEquals(1, result.clusteringSize());
        assertEquals(0, i(result.getClusteringColumn(0)));

        assertTrue(result.has(col("a")));
        assertTrue(result.has(col("b")));
        assertTrue(result.has(ccol("c1")));

        assertFalse(result.has(col("z")));
        assertFalse(result.has(ccol("c2")));

        assertEquals(3, result.size(ccol("c1")));

        assertEquals(3, ival(result, "a"));
        assertEquals(4, ival(result, "b"));
        assertEquals(0, ival(result, "c1", 0));
        assertEquals(0, ival(result, "c1", 1));
        assertEquals(1, ival(result, "c1", 2));

        assertEquals(1L, tstamp(result, "a"));
        assertEquals(0L, tstamp(result, "b"));
        assertEquals(1L, tstamp(result, "c1", 0));
        assertEquals(1L, tstamp(result, "c1", 1));
        assertEquals(2L, tstamp(result, "c1", 2));
    }
}
