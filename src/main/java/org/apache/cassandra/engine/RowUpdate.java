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

/**
 * Allows to provide updates for one row.
 */
public interface RowUpdate
{
    // TODO: Use a RowData underneath, but with a slightly modified way to find the size of
    // the Column, using one more ColumnPositions array to store the end.
    // Then we have some modified write helper that initialize all column positions to -1 initally,
    // and when we add a cell it just set it from the current position (or seek to the already set
    // position to override but that won't work for collections -- we can complain if the
    // same column is updated while it's not the current one).

    // TODO: should deal with default ttl (imply has an alias to it's metadata)
    public Row add(Column c, ByteBuffer value, long timestamp, int ttl);

    // TODO: this must handle collection deletion with a CollectionTombstone
    public Row delete(Column c, int localDeletionTime, long timestamp);

    // Deletes the whole Row (i.e. range tombstone)
    public Row delete(int localDeletionTime, long timestamp);

    public Row addInCollection(Column c, ByteBuffer key, ByteBuffer value, long timestamp, int ttl);
    public Row deleteInCollection(Column c, ByteBuffer key, int localDeletionTime, long timestamp);
}
