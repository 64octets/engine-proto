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
import java.util.Iterator;

/**
 * Represents an internal row.
 *
 * An internal row is identified by a "path" that is formed by it's clustering
 * columns values. An internal row then has a number of Columns (the non-PK
 * CQL columns) and each Column is composed of one or more cells. Simple
 * columns have only one corresponding cell while collection columns are
 * composed of multiple cells (one for each element of the collection).
 */
public interface Row extends Atom, Iterable<Cell>
{
    public int cellsCount();

    // Shorthand for size(c) > 0
    public boolean has(Column c);

    // The number of cells for the collection column c in this row. For regular columns
    // this method returns 1 as a convenience (and so this method return 0 if the column
    // is not in the row).
    public int size(Column c);

    // Get the cell corresponding to column c. Calls to those methods are allowed to result
    // the same cell object, so the returned object is only valid until the next get() call
    // on the same Row object (if you want to get a "persistent" cell, you will need to copy).
    public Cell get(Column c);
    public Cell get(Column c, int i);

    // Return an iterator over the cells of this Row. For performance reason, this method
    // is allowed to reuse the same iterator over calls. So calling this method a 2nd time
    // may (and generally will) invalidate the first iterator.
    public Iterator<Cell> iterator();
}
