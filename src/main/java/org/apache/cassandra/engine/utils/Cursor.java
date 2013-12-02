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
package org.apache.cassandra.engine.utils;

import org.apache.cassandra.engine.*;

/**
 * An cursor that allows navigation over a collection of Atoms.
 */
public interface Cursor extends Clusterable
{
    // Sets the element to position i.
    public void position(int i);

    // Gets the current position of the Cursor.
    public int position();

    // The limit of the collection this is a cursor over, i.e. the first
    // invalid position for this cursor.
    public int limit();

    public ClusteringComparator comparator();
}
