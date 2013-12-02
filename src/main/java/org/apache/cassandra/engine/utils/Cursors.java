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

import org.apache.cassandra.engine.Clusterable;

public abstract class Cursors
{
    private Cursors() {}

    /**
     * Moves a cursor to the provided element. If the element searched for is not
     * in the collection on which the cursor operates, the cursor is moved to
     * the first value greater than the one search (in other words, this is a binary
     * search).
     * <p>
     * Please note that the element is searched only from the current position of the
     * cursor. In other words, the entire underlying collection is only searched if
     * the cursor is positionned at it's beginning.
     *
     * @param toSearch the element to search for.
     * @param cursor the cursor to use for searching and to set ultimately.
     * @return {@code true} if element search was found (in which case the cursor is
     * moved to that element). If {@code false}, the cursor is set to the smallest element
     * greater than {@code toSearch}.
     */
    public static boolean moveTo(Clusterable toSearch, Cursor cursor)
    {
        int low = cursor.position() < 0 ? 0 : cursor.position();
        int high = cursor.limit() - 1;
        int result = -1;
        while (low <= high)
        {
            cursor.position((low + high) >> 1);
            if ((result = cursor.comparator().compare(toSearch, cursor)) > 0)
            {
                low = cursor.position() + 1;
            }
            else if (result == 0)
            {
                return true;
            }
            else
            {
                high = cursor.position() - 1;
            }
        }
        if (result > 0)
            cursor.position(cursor.position() + 1);
        return false;
    }
}
