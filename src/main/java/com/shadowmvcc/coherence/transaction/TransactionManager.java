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

package com.shadowmvcc.coherence.transaction;

import java.util.Collection;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.internal.MVCCNamedCache;

/**
 * {@code TransactionManager} implementations are responsible for creating {@link Transaction}
 * objects. Instances of {@link MVCCNamedCache} must be obtained from a {@code TransactionManager}
 * so that cache operations are performed within the correct transaction.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TransactionManager {
    
    /**
     * Construct and return an {@link MVCCNamedCache}.
     * @param cacheName the cache name
     * @return the cache
     */
    MVCCNamedCache getCache(String cacheName);
    
    /**
     * Ensure that the transaction manager is aware of other
     * caches that may be modified. Used in advance of invocations
     * of {@EntryProcessor} implementations that modify other caches through
     * their backing maps. 
     * @param referencedCaches names of caches that may be affected
     */
    void addReferencedCaches(Collection<CacheName> referencedCaches);
    
    /**
     * Get a NamedCache that will provide a view as at a given timestamp.
     * If the time is not later than the most recent snapshot, then it must be
     * a valid snapshot time. Reads on the view are performed with read committed
     * isolation for minimum overhead. 
     * @param cacheName the name of the cache
     * @param timestamp the view timestamp
     * @return the cache
     */
    MVCCNamedCache getTemporalCacheView(String cacheName, long timestamp);

    /**
     * Return the current transaction, constructs a new transaction if required.
     * @return the transaction
     */
    Transaction getTransaction();
    
}
