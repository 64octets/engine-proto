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

// TODO: this is probably not the interface we want. Just a moackup to see we can handle
// this during merge.
public interface IndexUpdater
{
    // A entirely new row is inserted
    public void insert(Row row);

    // A row in a memtable is updated by a new update. It's up to the updater to figure
    // which cells are updated (overriden or removed).
    public void update(Row old, Row row);

    // The row is entirely removed (due to being deleted by a range tombstone or partition level tombstone)
    public void remove(Row row);
}
