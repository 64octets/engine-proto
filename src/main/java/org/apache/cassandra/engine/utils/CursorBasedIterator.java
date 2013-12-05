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
package org.apache.cassandra.engine.utils;

import com.google.common.collect.AbstractIterator;

public class CursorBasedIterator<C extends Cursor> extends AbstractIterator<C>
{
    private static final EmptyIterator empty = new EmptyIterator();

    protected final C cursor;

    public CursorBasedIterator(C cursor)
    {
        this.cursor = cursor;
        if (cursor != null)
            reset();
    }

    public static <C extends Cursor> CursorBasedIterator<C> emptyIterator()
    {
        return (CursorBasedIterator<C>)empty;
    }

    protected void reset()
    {
        cursor.position(-1);
    }

    protected C computeNext()
    {
        cursor.position(cursor.position() + 1);
        return cursor.position() < cursor.limit() ? cursor : endOfData();
    }

    private static class EmptyIterator extends CursorBasedIterator<Cursor>
    {
        private EmptyIterator()
        {
            super(null);
        }

        @Override
        protected Cursor computeNext()
        {
            return endOfData();
        }
    }
}
