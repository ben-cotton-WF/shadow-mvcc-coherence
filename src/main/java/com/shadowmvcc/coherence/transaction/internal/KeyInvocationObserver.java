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

package com.shadowmvcc.coherence.transaction.internal;

import java.util.Queue;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.invocable.InvocationObserverStatus;
import com.tangosol.net.Member;

/**
 * Observer for key based invocations.
 *
 */
class KeyInvocationObserver implements InvocationObserverStatus {
    
    private final Set<Object> keyset;
    private final CacheName cachename;
    private final Queue<InvocationObserverStatus> resultQueue;
    private volatile boolean failed = false;

    /**
     * @param keyset set of keys
     * @param cachename cache name
     * @param resultQueue filter to place self on when complete
     */
    public KeyInvocationObserver(final Set<Object> keyset, final CacheName cachename,
            final Queue<InvocationObserverStatus> resultQueue) {
        super();
        this.keyset = keyset;
        this.cachename = cachename;
        this.resultQueue = resultQueue;
    }
    @Override
    public void memberCompleted(final Member member, final Object obj) {
    }
    @Override
    public void memberFailed(final Member member, final Throwable throwable) {
        failed = true;
    }
    @Override
    public void memberLeft(final Member member) {
        failed = true;
    }
    @Override
    public void invocationCompleted() {
        resultQueue.add(this);
    }
    /**
     * @return the cache name
     */
    public CacheName getCachename() {
        return cachename;
    }
    /**
     * @return the key set
     */
    public Set<Object> getKeys() {
        return keyset;
    }
    @Override
    public boolean isFailed() {
        return failed;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((cachename == null) ? 0 : cachename.hashCode());
        result = prime * result
                + ((keyset == null) ? 0 : keyset.hashCode());
        return result;
    }
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KeyInvocationObserver other = (KeyInvocationObserver) obj;
        if (cachename == null) {
            if (other.cachename != null) {
                return false;
            }
        } else if (!cachename.equals(other.cachename)) {
            return false;
        }
        if (keyset == null) {
            if (other.keyset != null) {
                return false;
            }
        } else if (!keyset.equals(other.keyset)) {
            return false;
        }
        return true;
    }
}