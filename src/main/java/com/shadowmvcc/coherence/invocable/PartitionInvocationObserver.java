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

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.invocable.InvocationServiceHelper.InvocableFactory;
import com.tangosol.net.partition.PartitionSet;

/**
 * Observer for partition-based invocations.
 *
 * @param <R> invocation result type
 */
public class PartitionInvocationObserver<R> extends InvocationObserverStatusImpl<PartitionSet, R> {
    
    /**
     * @param partitionset partition set invoked on
     * @param cachename cache name
     * @param resultQueue queue to place self on when complete
     * @param invocableFactory the invocable factory used to create the invocable being observed
     */
    public PartitionInvocationObserver(final PartitionSet partitionset, final CacheName cachename,
            final Queue<InvocationObserverStatus<?, R>> resultQueue,
            final InvocableFactory<PartitionSet> invocableFactory) {
        super(partitionset, cachename, resultQueue, invocableFactory);
    }
    
}