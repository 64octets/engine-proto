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

import com.google.common.base.Objects;

public abstract class DeletionTime implements Comparable<DeletionTime>
{
    public static final DeletionTime LIVE = new ImmutableDeletionTime(Long.MIN_VALUE, Integer.MAX_VALUE);

    public static DeletionTime createImmutable(long markedForDeleteAt, int localDeletionTime)
    {
        return new ImmutableDeletionTime(markedForDeleteAt, localDeletionTime);
    }

    public abstract long markedForDeleteAt();
    public abstract int localDeletionTime();

    public boolean isLive()
    {
        return markedForDeleteAt() == Long.MIN_VALUE
            && localDeletionTime() == Integer.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionTime))
            return false;
        DeletionTime that = (DeletionTime)o;
        return markedForDeleteAt() == that.markedForDeleteAt() && localDeletionTime() == that.localDeletionTime();
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(markedForDeleteAt(), localDeletionTime());
    }

    @Override
    public String toString()
    {
        return String.format("deletedAt=%d, localDeletion=%d", markedForDeleteAt(), localDeletionTime());
    }

    public int compareTo(DeletionTime dt)
    {
        if (markedForDeleteAt() < dt.markedForDeleteAt())
            return -1;
        else if (markedForDeleteAt() > dt.markedForDeleteAt())
            return 1;
        else if (localDeletionTime() < dt.localDeletionTime())
            return -1;
        else if (localDeletionTime() > dt.localDeletionTime())
            return -1;
        else
            return 0;
    }

    public boolean isGcAble(int gcBefore)
    {
        return localDeletionTime() < gcBefore;
    }

    private static class ImmutableDeletionTime extends DeletionTime
    {
        public final long markedForDeleteAt;
        public final int localDeletionTime;

        public ImmutableDeletionTime(long markedForDeleteAt, int localDeletionTime)
        {
            this.markedForDeleteAt = markedForDeleteAt;
            this.localDeletionTime = localDeletionTime;
        }

        public long markedForDeleteAt()
        {
            return markedForDeleteAt;
        }

        public int localDeletionTime()
        {
            return localDeletionTime;
        }
    }
}
