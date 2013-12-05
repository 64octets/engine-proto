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

// TODO: we probably want something a bit more fancy, especially
// in term of ctor.
public class ClusteringPrefix implements Clusterable
{
    protected final ByteBuffer[] prefix;

    public ClusteringPrefix(ByteBuffer... values)
    {
        this.prefix = values;
    }

    public int clusteringSize()
    {
        return prefix.length;
    }

    public ByteBuffer getClusteringColumn(int i)
    {
        return prefix[i];
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ClusteringPrefix))
            return false;

        ClusteringPrefix that = (ClusteringPrefix)other;
        if (this.prefix.length != that.prefix.length)
            return false;

        for (int i = 0; i < prefix.length; i++)
            if (!this.prefix[i].equals(that.prefix[i]))
                return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(prefix);
    }
}
