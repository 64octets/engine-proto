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
 * Represents an internal row.
 *
 * An internal row is identified by a "path" that is formed by it's clustering
 * columns values. An internal row then has a number of Columns (the non-PK
 * CQL columns) and each Column is composed of one or more cells. Simple
 * columns have only one corresponding cell while collection columns are
 * composed of multiple cells (one for each element of the collection).
 *
 * Each cell has a value, a timestamp, a ttl and a local deletion time. On top
 * of that, collection cells have a key (that identify the cell within the
 * collection). The data for a given cell can be accessed through it's "position".
 * For regular columns, the position() method return the position of it's
 * corresponding cell. For collections, the position() method return the position
 * of the first cell representing the collection, position() + 1 is the 2nd
 * element etc... The number of cell for a collection column is provided by the
 * size method.
 *
 * Note that the positions for the cells in the row are guaranteed to be sequential,
 * so that to iterate over all the cells of a Row, you can do:
 *   for (int pos = startPosition(); i < startPosition() + cellsCount(); i++)
 *      // ... access cell at position pos ...
 *
 * But you should *not* assume that startPosition() is 0.
 *
 * Note that columns returned by such iteration are guaranteed to be in Column order.
 */
public interface Row extends Atom
{
    // TODO: We need a good way to know if a cell is shadowed or not. Maybe shadowed cells
    // should just be removed right away and never be stored, meaning that merge iterator
    // would just ignore shadowed stuffs (and if memtable inserts don't use the iterators,
    // then whatever solution needs to do the same).

    public int startPosition();
    public int endPosition();
    public int cellsCount();

    // Shorthand for position(c) >= 0
    public boolean has(Column c);

    // The returned value is < 0 if the Column is not in this row.
    public int position(Column c);

    // The number of cells for the collection column c in this row. For regular columns
    // this method returns 1 as a convenience (and so this method return 0 if the column
    // is not in the row).
    public int size(Column c);

    public Column columnForPosition(int pos);

    // Whether or not the cell at position pos is deleted given the current time, i.e. whether
    // it is a tombstone or an expired cell.
    public boolean isDeleted(int pos, int now);

    public boolean isTombstone(int pos);
    public ByteBuffer value(int pos);
    public long timestamp(int pos);
    public int ttl(int pos);
    public long localDeletionTime(int pos);

    // For collections cells
    public ByteBuffer key(int pos);

    //public long timestampOfLastDelete(Column c); // For counters
}
