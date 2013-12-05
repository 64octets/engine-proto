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

import java.io.IOException;

import com.google.common.collect.AbstractIterator;

/**
 * Abstract class to simplify writing AtomIterators that wrap another iterator
 * but filter parts of it.
 */
public abstract class AbstractFilteringAtomIterator extends AbstractIterator<Atom> implements AtomIterator
{
    protected final AtomIterator filteredIterator;

    protected AbstractFilteringAtomIterator(AtomIterator filteredIterator)
    {
        this.filteredIterator = filteredIterator;
    }
    protected abstract RangeTombstone filter(RangeTombstone rt);
    protected abstract Row filter(Row row);

    // Can be overriden to make the filtering iterator stop even if the filtered one has more data
    // Note: this is only call once for each call to the filtering iterator, not the filtered one.
    protected boolean isDone()
    {
        return false;
    }

    protected Atom computeNext()
    {
        if (isDone())
            return endOfData();

        while (filteredIterator.hasNext())
        {
            Atom atom = filteredIterator.next();
            Atom filteredAtom = null;
            switch (atom.kind())
            {
                case ROW:
                    filteredAtom = filter((Row)atom);
                    break;
                case RANGE_TOMBSTONE:
                    filteredAtom = filter((RangeTombstone)atom);
                    break;
                case COLLECTION_TOMBSTONE:
                    throw new UnsupportedOperationException();
            }
            if (filteredAtom != null)
                return filteredAtom;
        }
        return endOfData();
    }

    public Layout metadata()
    {
        return filteredIterator.metadata();
    }

    public DecoratedKey getPartitionKey()
    {
        return filteredIterator.getPartitionKey();
    }

    public DeletionTime partitionLevelDeletion()
    {
        return filteredIterator.partitionLevelDeletion();
    }

    public void close() throws IOException
    {
        filteredIterator.close();
    }
}
