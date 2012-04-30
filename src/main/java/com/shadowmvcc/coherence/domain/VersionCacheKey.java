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

package com.shadowmvcc.coherence.domain;

import com.shadowmvcc.coherence.cache.CacheName;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Identify the cache and key of a version cache entry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> logical key type
 */
@Portable
public class VersionCacheKey<K> {
    
    @PortableProperty(0) private CacheName cacheName;
    @PortableProperty(1) private VersionedKey<K> key;
    
    /**
     *  Default constructor for POF use only.
     */
    public VersionCacheKey() {
        super();
    }
    
    /**
     * Constructor.
     * @param cacheName cache name
     * @param key version cache key
     */
    public VersionCacheKey(final CacheName cacheName, final VersionedKey<K> key) {
        super();
        this.cacheName = cacheName;
        this.key = key;
    }
    
    /**
     * Get the name of the cache this key belongs to.
     * @return the cache name
     */
    public CacheName getCacheName() {
        return cacheName;
    }

    /**
     * Get the version cache key.
     * @return the version cache key
     */
    public VersionedKey<K> getKey() {
        return key;
    }
}
