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
import java.util.BitSet;


// TODO: We should specialize for dense rows, where we have only one cell per row.
public class ReusableRow extends AbstractRow
{
    private final RowData data;
    private Writer writer;

    public ReusableRow(Layout layout, int initialCapacity)
    {
        this.data = new RowData(layout, 1, initialCapacity);
    }

    protected RowData data()
    {
        return data;
    }

    protected int row()
    {
        return 0;
    }

    public Writer writer()
    {
        if (writer == null)
            writer = new Writer(data);
        else
            writer.reset(); // We want to alway reuse the same row
        return writer;
    }
}
