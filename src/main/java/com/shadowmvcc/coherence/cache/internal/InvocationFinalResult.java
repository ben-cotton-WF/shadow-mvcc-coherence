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

package com.shadowmvcc.coherence.cache.internal;

import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;

/**
 * Result type for filter based ntryProcessor invocations. Encapsulates the
 * map of key/result pairs for the entryprocessor invocations and a set of keys that were
 * changed. The changed keys may contain entries not in the map if the EntryProcessor processAll()
 * method did not return them. It may also omit keys in the result map where these cache
 * entries were not modified by the EntrProcessor.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> key type of the cache
 * @param <R> the result type of the EntryProcessor process method
 */
public class InvocationFinalResult<K, R> {
    private final Map<K, R> resultMap;
    @SuppressWarnings("rawtypes")
    private final Map<CacheName, Set> changedKeys;
    
    /** Constructor.
     * @param resultMap map of EntryProcessor results
     * @param changedKeys set of keys changed by the invocation
     */
    public InvocationFinalResult(final Map<K, R> resultMap,
            @SuppressWarnings("rawtypes") final Map<CacheName, Set> changedKeys) {
        super();
        this.resultMap = resultMap;
        this.changedKeys = changedKeys;
    }
    /**
     * Get the map of processor results.
     * @return the map of processor results
     */
    public Map<K, R> getResultMap() {
        return resultMap;
    }
    /**
     * Get the set of keys changed by the invocation.
     * @return the set of changed keys
     */
    @SuppressWarnings("rawtypes")
    public Map<CacheName, Set> getChangedKeys() {
        return changedKeys;
    }
    
}