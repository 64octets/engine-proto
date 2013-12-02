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

import java.util.Comparator;

public abstract class AbstractClusteringComparator implements ClusteringComparator
{
    private final Comparator<Atom> atomComparator;

    public AbstractClusteringComparator()
    {
        // Sort first by clustering prefix. For the same clustering prefix,
        // sorts range tombstone first, collection tombstone next and cell group last.
        // For cell group, same prefix means compare equaly. For tombstones, we break
        // the ties in the most reasonable way.
        this.atomComparator = new Comparator<Atom>()
        {
            public int compare(Atom a1, Atom a2)
            {
                int cmp = AbstractClusteringComparator.this.compare(a1, a2);
                if (cmp != 0)
                    return cmp;

                switch (a1.kind())
                {
                    case ROW:
                        return a2.kind() == Atom.Kind.ROW ? 0 : 1;
                    case RANGE_TOMBSTONE:
                        if (a2.kind() != Atom.Kind.RANGE_TOMBSTONE)
                            return 1;
                        RangeTombstone rt1 = (RangeTombstone)a1;
                        RangeTombstone rt2 = (RangeTombstone)a2;
                        int endCmp = AbstractClusteringComparator.this.compare(rt1.max(), rt2.max());
                        if (endCmp != 0)
                            return endCmp;
                        return rt1.delTime().compareTo(rt2.delTime());
                    case COLLECTION_TOMBSTONE:
                        switch (a2.kind())
                        {
                            case ROW: return -1;
                            case RANGE_TOMBSTONE: return 1;
                            case COLLECTION_TOMBSTONE: return 0; // TODO: likely not what we want
                        }
                }
                throw new AssertionError();
            }
        };
    }

    public Comparator<Atom> atomComparator()
    {
        return atomComparator;
    }
}
