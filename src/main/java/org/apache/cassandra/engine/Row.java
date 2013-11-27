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

public interface Row extends Clusterable
{
    public int cellsCount();

    public boolean has(Column c);

    // For non-collection columns
    public boolean isDeleted(Column c, int now);
    public ByteBuffer value(Column c);
    public long timestamp(Column c);
    public int ttl(Column c);
    public long localDeletionTime(Column c);

    public long timestampOfLastDelete(Column c); // For counters

    // Collections columns
    public int size(Column c);
    public boolean isDeleted(Column c, int i, int now);
    public ByteBuffer key(Column c, int i);
    public ByteBuffer value(Column c, int i);
    public long timestamp(Column c, int i);
    public int ttl(Column c, int i);
    public long localDeletionTime(Column c, int i);
}
