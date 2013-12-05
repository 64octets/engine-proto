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

public class CloningRangeTombstone extends RangeTombstone
{
    private final Allocator allocator;
    private final Layout metadata;

    private final CloningClusteringPrefix min;
    private final CloningClusteringPrefix max;
    private DeletionTime delTime;

    public CloningRangeTombstone(Layout metadata, Allocator allocator)
    {
        this.metadata = metadata;
        this.allocator = allocator;
        this.min = new CloningClusteringPrefix(metadata.clusteringSize());
        this.max = new CloningClusteringPrefix(metadata.clusteringSize());
    }

    public void set(ClusteringPrefix min, ClusteringPrefix max, DeletionTime delTime)
    {
        this.min.copy(min);
        this.max.copy(max);
        this.delTime = delTime;
    }

    public Layout metadata()
    {
        return metadata;
    }

    public ClusteringPrefix min()
    {
        return min;
    }

    public ClusteringPrefix max()
    {
        return max;
    }

    public DeletionTime delTime()
    {
        return delTime;
    }

    private class CloningClusteringPrefix extends ClusteringPrefix
    {
        private int size;

        public CloningClusteringPrefix(int maxSize)
        {
            super(new ByteBuffer[maxSize]);
        }

        public void copy(ClusteringPrefix toCopy)
        {
            size = toCopy.clusteringSize();
            for (int i = 0; i < size; i++)
                prefix[i] = allocator.clone(toCopy.getClusteringColumn(i));
        }

        @Override
        public int clusteringSize()
        {
            return size;
        }
    }
}
