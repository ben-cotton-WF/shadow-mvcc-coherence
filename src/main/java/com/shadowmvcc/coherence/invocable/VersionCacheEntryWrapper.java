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


import com.shadowmvcc.coherence.domain.VersionedKey;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;

/**
 * Wrapper around a single version cache entry to make it look
 * like a logical cache entry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> logical key type
 * @param <V> version type
 */
public class VersionCacheEntryWrapper<K, V> implements Entry {

    private final Entry underlying;

    /**
     * Constructor.
     * @param underlying version cache entry
     */
    public VersionCacheEntryWrapper(final Entry underlying) {
        super();
        this.underlying = underlying;
    }

    @SuppressWarnings("unchecked")
    @Override
    public K getKey() {
        return ((VersionedKey<K>) underlying.getKey()).getNativeKey();
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getValue() {
        return (V) underlying.getValue();
    }

    @Override
    public V setValue(final Object obj) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public Object extract(final ValueExtractor valueextractor) {
        return valueextractor.extract(getValue());
    }

    @Override
    public void setValue(final Object obj, final boolean flag) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public void update(final ValueUpdater valueupdater, final Object obj) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public boolean isPresent() {
        return underlying.isPresent();
    }

    @Override
    public void remove(final boolean flag) {
        throw new UnsupportedOperationException("read only entry");
    }

}
