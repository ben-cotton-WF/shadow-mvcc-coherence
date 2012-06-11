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

import com.shadowmvcc.coherence.invocable.InvocationServiceHelper.InvocableFactory;
import com.tangosol.net.InvocationObserver;

/**
 * Extension for an {@code InvocationObserver} that records its own
 * success or failure state.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <P> type of the target of the invocation.
 * @param <R> result type of the invocation
 */
public interface InvocationObserverStatus<P, R> extends InvocationObserver {
    
    /**
     * Has the observed invocation failed.
     * @return true if the invocation has failed
     */
    boolean isFailed();

    /**
     * Get the invocation target.
     * @return the invocation target
     */
    P getInvocationTarget();

    /**
     * Get the cause of failure.
     * @return the cause, or null if none.
     */
    Throwable getFailureCause();

    /**
     * Get the factory used to create the invocable being monitored.
     * @return the invocable factory
     */
    InvocableFactory<P> getInvocableFactory();
    
    /**
     * Get the invocation result, or null if it failed.
     * @return the invocation result
     */
    R getResult();

}
