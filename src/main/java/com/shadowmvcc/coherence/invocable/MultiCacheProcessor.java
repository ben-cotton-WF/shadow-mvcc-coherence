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

import com.shadowmvcc.coherence.cache.CacheName;

/**
 * Interface used to identify that an {@code EntryProcessor} will engage in updates to the
 * backing maps of MVCC Caches than the one it is invoked against. Using this interface
 * servies two purposes:
 * <ul>
 * <li>Informs the invocation wrapper components that a request for a backing
 * map on one of these caches should be wrapped to provide the logical cache
 * view of an MVCC cache</li>
 * <li>Marks the transaction with the given cache names so that the failed client
 * logic will correctly complete or rollback updates to these caches. It is necessary
 * for the cache names to be known before the {@code EntryProcessor} is invoked, hence the need
 * for this interface.</li>
 * </ul>
 * Any backing map access to caches not included in the returned collection
 * will be treated as access to non-MVCC caches, and changes to these will be
 * effective on completion of the EntryProcessor invocation and will not
 * participate in MVCC transactions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface MultiCacheProcessor {
    /**
     * Get the collection of MVCC cache names whose backing maps
     * may be referenced or modified by this {@code EntryProcessor}.
     * @return the collection of MVCC cache names affected
     */
    Collection<CacheName> getReferencedMVCCCacheNames();

}
