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
import java.util.Iterator;

import com.google.common.collect.Iterators;

public abstract class AbstractRow implements Row
{
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;
    public static final int NO_TTL = 0;
    public static final long NO_LOCAL_DELETION_TIME = Long.MAX_VALUE;

    private RowData.ReusableCell cellForGet;
    private RowData.ReusableCell cellForIteration;

    protected abstract int row();
    protected abstract RowData data();

    public Atom.Kind kind()
    {
        return Atom.Kind.ROW;
    }

    public Iterator<Cell> iterator()
    {
        if (cellForIteration == null)
            cellForIteration = data().createCell();

        if (cellForIteration.currentRow() != row())
            cellForIteration.setToRow(row());

        return cellForIteration;
    }

    public Cell get(Column c)
    {
        return get(c, 0);
    }

    public Cell get(Column c, int i)
    {
        if (cellForGet == null)
            cellForGet = data().createCell();

        if (cellForGet.currentRow() != row())
            cellForGet.setToRow(row());

        cellForGet.setToCell(c, i);
        return cellForGet;
    }

    public Layout metadata()
    {
        return data().metadata();
    }

    public int clusteringSize()
    {
        return data().clusteringSize();
    }

    public ByteBuffer getClusteringColumn(int i)
    {
        return data().clusteringColumn(row(), i);
    }

    public int cellsCount()
    {
        return data().cellsCount(row());
    }

    public boolean has(Column c)
    {
        return size(c) > 0;
    }

    public int size(Column c)
    {
        return data().size(row(), c);
    }

    protected RowData.ReusableCell getReusableCellForWrite()
    {
        if (cellForIteration == null)
            cellForIteration = data().createCell();
        return cellForIteration;
    }

    public Rows.Writer writer()
    {
        return getReusableCellForWrite();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Row))
            return false;

        Row left = this;
        Row right = (Row)other;

        if (left.clusteringSize() != right.clusteringSize())
            return false;

        for (int i = 0; i < left.clusteringSize(); i++)
            if (!left.getClusteringColumn(i).equals(right.getClusteringColumn(i)))
                return false;

        return Iterators.elementsEqual(left.iterator(), right.iterator());
    }
}
