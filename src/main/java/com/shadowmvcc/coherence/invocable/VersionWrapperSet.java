/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

package com.shadowmvcc.coherence.invocable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;

/**
 * Wrapper around a set of version cache binary entries to make it look
 * like a set of logical cache binary entries.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class VersionWrapperSet implements Set<Entry> {
    private final Set<Entry> versionedSet;

    /**
     * Constructor.
     * @param versionedSet the set of version cache entries
     */
    public VersionWrapperSet(final Set<Entry> versionedSet) {
        super();
        this.versionedSet = versionedSet;
    }

    @Override
    public int size() {
        return versionedSet.size();
    }

    @Override
    public boolean isEmpty() {
        return versionedSet.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException("thought this wouldn't be called");
    }

    @Override
    public Iterator<Entry> iterator() {
        return new Iterator<Entry>() {

            private Iterator<Entry> underlying = versionedSet.iterator();

            @Override
            public boolean hasNext() {
                return underlying.hasNext();
            }

            @SuppressWarnings({ "rawtypes" })
            @Override
            public Entry next() {
                Entry underlyingEntry = underlying.next();
                if (underlyingEntry instanceof BinaryEntry) {
                    return new VersionCacheBinaryEntryWrapper((BinaryEntry) underlyingEntry);
                } else {
                    return new VersionCacheEntryWrapper(underlyingEntry);
                }
            }

            @Override
            public void remove() {
                underlying.remove();
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("thought this wouldn't be called");
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        throw new UnsupportedOperationException("thought this wouldn't be called");
    }

    @Override
    public boolean add(final Entry e) {
        throw new UnsupportedOperationException("set is read only");
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException("set is read only");
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        throw new UnsupportedOperationException("thought this wouldn't be called");
    }

    @Override
    public boolean addAll(final Collection<? extends Entry> c) {
        throw new UnsupportedOperationException("set is read only");
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException("set is read only");
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException("set is read only");
    }

    @Override
    public void clear() {
        versionedSet.clear();
    }

    @Override
    public boolean equals(final Object o) {
        return versionedSet.equals(o);
    }

    @Override
    public int hashCode() {
        return versionedSet.hashCode();
    }

}
