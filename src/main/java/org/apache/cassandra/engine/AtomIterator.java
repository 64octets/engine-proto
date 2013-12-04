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

import java.io.Closeable;
import java.util.Iterator;

/**
 * An iterator over the Atoms of a given partition.
 * <p>
 * The Atom object returned by a call to next() is only guaranteed to be valid until
 * the next call to hasNext() or next(). In other words, consumers needs to be warry to
 * not keep references to the returned objects.
 * <p>
 * An implementation of AtomIterator *must* provide the following guarantees:
 *   1) the returned Atom should be in clustering order (or reverse clustering order).
 *   2) the tombstone (range and collections) returned by the iterator should *not* shadow
 *   any cell of the Row returned by the iterator. Concretely, this means the range
 *   tombstones returned by a given iterator may shadow cells from another iterator (and
 *   in particular AtomIterator merging take this into account), but not the
 *   cells returned by the iterator itself.
 */
public interface AtomIterator extends Iterator<Atom>, Closeable
{
    public Layout metadata();
    public DecoratedKey getPartitionKey();

    public DeletionTime partitionLevelDeletion();
}
