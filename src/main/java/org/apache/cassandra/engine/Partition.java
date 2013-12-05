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

public interface Partition //implements Iterable<Row>
{
    public Layout metadata();
    public ClusteringComparator comparator();

    public DecoratedKey getPartitionKey();
    public DeletionInfo deletionInfo();

    // No values (even deleted), live deletion infos
    public boolean isEmpty();

    // Number of rows in this partition.
    public int rowCount();

    // Use sparingly, prefer iterator()/atomIterator when possible
    public Row findRow(RowPath path);

    // The Row objects returned are only valid until next() is called!
    // This iterator and the Row object it returns skips deleted values.
    // For the CQL code or other high level.
    //public Iterator<Row> iterator(int now);

    // Iterator over the Atom contained in this partition.
    public AtomIterator atomIterator();
    public AtomIterator atomIterator(Slices slices);
}
