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
import java.util.Arrays;
import java.util.Comparator;

import org.junit.Test;
import static org.junit.Assert.*;

import static org.apache.cassandra.engine.TestUtils.*;

public class AtomIteratorsTest
{
    @Test
    public void testMerging()
    {
        Layout layout = simpleLayout();

        // First Row
        Row row11 = newRowWriter(layout).clustering(0)
                                        .add("a", 3, 1L)
                                        .add("b", 2, 1L)
                                        .add("c1", 0, 0, 0L)
                                        .add("c1", 1, 1, 1L)
                                        .done();
        Row row12 = newRowWriter(layout).clustering(0)
                                        .add("a", 5, 1L)
                                        .add("c1", 0, 2, 0L)
                                        .add("c1", 1, 4, 3L)
                                        .add("c1", 2, 1, 1L)
                                        .add("c2", 0, 1, 1L)
                                        .done();
        Row row13 = newRowWriter(layout).clustering(0)
                                        .add("b", 3, 2L)
                                        .add("c1", 0, 4, 0L)
                                        .add("z", 3, 0L)
                                        .done();

        Row row1 = newRowWriter(layout).clustering(0)
                                       .add("a", 3, 1L)
                                       .add("b", 3, 2L)
                                       .add("c1", 0, 0, 0L)
                                       .add("c1", 1, 4, 3L)
                                       .add("c1", 2, 1, 1L)
                                       .add("c2", 0, 1, 1L)
                                       .add("z", 3, 0L)
                                       .done();

        RangeTombstone rt = rt(4, 12, 3L);

        // Second Row -- should be entirely deleted by range tombstone
        Row row22 = newRowWriter(layout).clustering(5)
                                        .add("a", 5, 1L)
                                        .add("b", 5, 0L)
                                        .add("c1", 0, 2, 3L)
                                        .done();


        // Third Row -- only partially deleted by range tombstone
        Row row31 = newRowWriter(layout).clustering(10)
                                        .add("b", 2, 4L)
                                        .add("c1", 0, 0, 1L)
                                        .add("c2", 0, 3, 5L)
                                        .add("z", 2, 5L)
                                        .done();
        Row row33 = newRowWriter(layout).clustering(10)
                                        .add("b", 5, 1L)
                                        .add("c2", 0, 5, 6L)
                                        .add("c2", 1, 0, 3L)
                                        .add("z", 2, 5L)
                                        .done();

        Row row3 = newRowWriter(layout).clustering(10)
                                       .add("b", 2, 4L)
                                       .add("c2", 0, 5, 6L)
                                       .add("z", 2, 5L)
                                       .done();

        // 3 input iterator
        AtomIterator iter1 = newAtomIteratorBuilder(layout, dk(0)).add(row11).add(rt).add(row31).build();
        AtomIterator iter2 = newAtomIteratorBuilder(layout, dk(0)).add(row12).add(row22).build();
        AtomIterator iter3 = newAtomIteratorBuilder(layout, dk(0)).add(row13).add(row33).build();

        // On result iterator
        AtomIterator result = newAtomIteratorBuilder(layout, dk(0)).add(row1).add(rt).add(row3).build();

        AtomIterator merged = AtomIterators.merge(Arrays.asList(iter1, iter2, iter3), 0);
        assertSameIterator(result, merged);
    }
}
