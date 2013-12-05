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

import java.util.Iterator;

import org.apache.cassandra.engine.utils.*;

public abstract class Slices
{
    public static final Slices SELECT_ALL = new SelectAll();

    // TODO: probably require a bit of work
    public boolean isReversed()
    {
        return false;
    }

    public static Slices create(ClusteringPrefix start, ClusteringPrefix end)
    {
        // TODO: it would be be possible to has a simpler implementation for the 1 slice special
        // case (but it's unclear it's worth the trouble).
        return new ArrayBacked(new ClusteringPrefix[]{ start }, new ClusteringPrefix[]{ end });
    }

    /**
     * Given a cursor, returns a CursorBasedIterator on that cursor that only return elements within
     * those slices.
     */
    public abstract <C extends Cursor> Iterator<C> makeIterator(C c);

    private static class ArrayBacked extends Slices
    {
        private final ClusteringPrefix[] starts;
        private final ClusteringPrefix[] ends;

        private ArrayBacked(ClusteringPrefix[] starts, ClusteringPrefix[] ends)
        {
            assert starts.length == ends.length;
            this.starts = starts;
            this.ends = ends;
        }

        public <C extends Cursor> Iterator<C> makeIterator(C c)
        {
            return new CursorBasedIterator<C>(c)
            {
                private int currentSlice;
                private boolean inSlice;

                @Override
                protected C computeNext()
                {
                    cursor.position(cursor.position() + 1);
                    while (true)
                    {
                        if (currentSlice >= starts.length || cursor.position() >= cursor.limit())
                            return endOfData();

                        if (inSlice)
                        {
                            // is the next still in the current slice?
                            if (cursor.comparator().compare(cursor, ends[currentSlice]) <= 0)
                                return cursor;
                            inSlice = false;
                            ++currentSlice;
                        }
                        else
                        {
                            // seach the first element for the current slice
                            Cursors.moveTo(starts[currentSlice], cursor);
                            inSlice = true;
                        }
                    }
                }
            };
        }
    }

    private static class SelectAll extends Slices
    {
        public <C extends Cursor> Iterator<C> makeIterator(C c)
        {
            return new CursorBasedIterator<C>(c);
        }
    }
}
