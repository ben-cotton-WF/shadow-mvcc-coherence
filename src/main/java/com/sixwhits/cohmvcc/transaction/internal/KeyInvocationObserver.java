package com.sixwhits.cohmvcc.transaction.internal;

import java.util.Queue;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.invocable.InvocationObserverStatus;
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