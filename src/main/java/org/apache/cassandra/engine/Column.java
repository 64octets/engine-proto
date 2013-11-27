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

/**
 * Defines a CQL column.
 */
public class Column implements Comparable<Column>
{
    private final String name;
    private final boolean isCollection;

    public Column(String name, boolean isCollection)
    {
        this.name = name;
        this.isCollection = isCollection;
    }

    public String name()
    {
        return name;
    }

    public boolean isCounter()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isCollection()
    {
        return isCollection;
    }

    public int compareTo(Column column)
    {
        if (this == column)
            return 0;
        return name.compareTo(column.name);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if (!(other instanceof Column))
            return false;
        return name.equals(((Column)other).name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        return name;
    }
}
