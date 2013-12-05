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

public class ArrayBackedPartitionTest
{
    private static final Layout layout = simpleLayout();

    @Test
    public void testCreation()
    {
        AtomIteratorBuilder builder = newAtomIteratorBuilder(layout, dk(0));
        builder.add(getRow1()).add(rt(1, 4, 2)).add(getRow2()).add(getRow3());

        validatePartition(ArrayBackedPartition.accumulate(builder.build(), 3, 10), builder);
        validatePartition(ArrayBackedPartition.accumulate(builder.build(), 1, 1), builder);
    }

    @Test
    public void testSliceIterator()
    {
        AtomIteratorBuilder builder = newAtomIteratorBuilder(layout, dk(0));
        builder.add(getRow1()).add(rt(1, 4, 2)).add(getRow2()).add(getRow3());

        Partition p = ArrayBackedPartition.accumulate(builder.build(), 3, 10);
        Slices slices;

        // [0, 10] -- should fetch all
        assertSameIterator(builder.build(), p.atomIterator(slices(0, 10)));

        // [1, 7] -- RT and row2
        builder = newAtomIteratorBuilder(layout, dk(0)).add(rt(1, 4, 2)).add(getRow2());
        assertSameIterator(builder.build(), p.atomIterator(slices(1, 7)));

        // [1, 11] -- all except row1
        builder = newAtomIteratorBuilder(layout, dk(0)).add(rt(1, 4, 2)).add(getRow2()).add(getRow3());
        assertSameIterator(builder.build(), p.atomIterator(slices(1, 11)));

        // [6, 15] -- only row3
        builder = newAtomIteratorBuilder(layout, dk(0)).add(getRow3());
        assertSameIterator(builder.build(), p.atomIterator(slices(6, 15)));

        // [2, 4] -- nothing
        builder = newAtomIteratorBuilder(layout, dk(0));
        assertSameIterator(builder.build(), p.atomIterator(slices(2, 4)));

        // [11, 15] -- nothing
        builder = newAtomIteratorBuilder(layout, dk(0));
        assertSameIterator(builder.build(), p.atomIterator(slices(11, 15)));
    }

    private static Row getRow1()
    {
        return newRowWriter(layout).clustering(0)
                                   .add("a", 3, 1L)
                                   .add("b", 2, 1L)
                                   .add("c1", 0, 0, 1L)
                                   .add("c1", 1, 1, 1L)
                                   .done();
    }

    private static void validateRow1(Row row)
    {
        assertTrue(row.has(col("a")));
        assertTrue(row.has(col("b")));
        assertTrue(row.has(ccol("c1")));

        assertFalse(row.has(ccol("c2")));
        assertFalse(row.has(col("z")));

        assertEquals(2, row.size(ccol("c1")));

        assertEquals(3, ival(row, "a"));
        assertEquals(2, ival(row, "b"));
        assertEquals(0, ival(row, "c1", 0));
        assertEquals(1, ival(row, "c1", 1));

        assertEquals(1L, tstamp(row, "a"));
        assertEquals(1L, tstamp(row, "b"));
        assertEquals(1L, tstamp(row, "c1", 0));
        assertEquals(1L, tstamp(row, "c1", 1));
    }

    private static Row getRow2()
    {
        return newRowWriter(layout).clustering(5)
                                   .add("a", 4, 1L)
                                   .add("b", 5, 1L)
                                   .add("z", 6, 1L)
                                   .done();
    }

    private static void validateRow2(Row row)
    {
        assertTrue(row.has(col("a")));
        assertTrue(row.has(col("b")));
        assertTrue(row.has(col("z")));

        assertFalse(row.has(ccol("c1")));
        assertFalse(row.has(ccol("c2")));

        assertEquals(4, ival(row, "a"));
        assertEquals(5, ival(row, "b"));
        assertEquals(6, ival(row, "z"));

        assertEquals(1L, tstamp(row, "a"));
        assertEquals(1L, tstamp(row, "b"));
        assertEquals(1L, tstamp(row, "z"));
    }

    private static Row getRow3()
    {
        return newRowWriter(layout).clustering(10)
                                   .add("c1", 0, 3, 1L)
                                   .add("c2", 0, 5, 1L)
                                   .add("c2", 1, 6, 1L)
                                   .done();
    }

    private static void validateRow3(Row row)
    {
        assertTrue(row.has(ccol("c1")));
        assertTrue(row.has(ccol("c2")));

        assertFalse(row.has(col("a")));
        assertFalse(row.has(col("b")));
        assertFalse(row.has(col("z")));

        assertEquals(1, row.size(ccol("c1")));
        assertEquals(2, row.size(ccol("c2")));

        assertEquals(3, ival(row, "c1", 0));
        assertEquals(5, ival(row, "c2", 0));
        assertEquals(6, ival(row, "c2", 1));

        assertEquals(1L, tstamp(row, "c1", 0));
        assertEquals(1L, tstamp(row, "c2", 0));
        assertEquals(1L, tstamp(row, "c2", 1));
    }

    private static void validatePartition(Partition p, AtomIteratorBuilder builder)
    {
        assertEquals(dk(0), p.getPartitionKey());
        assertFalse(p.isEmpty());

        assertEquals(3, p.rowCount());

        validateRow1(p.findRow(path(0)));
        validateRow2(p.findRow(path(5)));
        validateRow3(p.findRow(path(10)));
        assertEquals(null, p.findRow(path(1)));

        assertSameIterator(builder.build(), p.atomIterator());
    }
}
