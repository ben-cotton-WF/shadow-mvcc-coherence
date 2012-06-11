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

import java.util.Queue;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.invocable.InvocationServiceHelper.InvocableFactory;

/**
 * Observer for key based invocations.
 * 
 * @param <K> type of key invoked against
 * @param <R> result type of Invocable
 *
 */
public class KeyInvocationObserver<K, R> extends InvocationObserverStatusImpl<Set<K>, R> {

    /**
     * Constructor.
     * @param invocationTarget key set invoked against
     * @param cachename cache name
     * @param resultQueue result queue
     * @param invocableFactory the invocable factory used to create the invocable being observed
     */
    public KeyInvocationObserver(final Set<K> invocationTarget,
            final CacheName cachename,
            final Queue<InvocationObserverStatus<?, R>> resultQueue,
            final InvocableFactory<Set<K>> invocableFactory) {
        super(invocationTarget, cachename, resultQueue, invocableFactory);
    }
    
    
}