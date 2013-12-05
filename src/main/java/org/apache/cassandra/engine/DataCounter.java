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
 * Object in charge of tracking if we have fetch enough data for a given query.
 * <p>
 * The reason this is not just a simple integer is that Thrift and CQL3 count
 * stuffs in different ways. This is what abstract those differences.
 */
public interface DataCounter
{
    // Calls when we start counting a new partition.
    // hasEnoughDataForPartition() depends on the last time we called that.
    public void countPartition();

    // Count a new row given it's number of live cells and tombstoned ones.
    public void countRow(int liveCells, int tombstonedCells);

    // Whether we've collected enough total results
    public boolean hasEnoughData();

    // Whether we've collected enough results for the current partition
    public boolean hasEnoughDataForPartition();

    // The total maximum of rows to fetch for this counter.
    // (for CQL3, this is basically the user LIMIT, but for thrift it's 'nb partitions' * 'nb cells per partitions')
    public int maxRowsToFetch();
}
