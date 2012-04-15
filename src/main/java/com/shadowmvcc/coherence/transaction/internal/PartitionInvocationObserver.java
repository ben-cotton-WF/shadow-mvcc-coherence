package com.shadowmvcc.coherence.transaction.internal;

import java.util.Queue;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.invocable.InvocationObserverStatus;
import com.tangosol.net.Member;
import com.tangosol.net.partition.PartitionSet;

/**
 * Observer for partition-based invocations.
 *
 */
class PartitionInvocationObserver implements InvocationObserverStatus {
    
    private final PartitionSet partitionset;
    private final CacheName cachename;
    private final Queue<InvocationObserverStatus> resultQueue;
    private volatile boolean failed = false;

    /**
     * @param partitionset partition set invoked on
     * @param cachename cache name
     * @param resultQueue queue to place self on when complete
     */
    public PartitionInvocationObserver(final PartitionSet partitionset, final CacheName cachename,
            final Queue<InvocationObserverStatus> resultQueue) {
        super();
        this.partitionset = partitionset;
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
     * @return the partition set
     */
    public PartitionSet getPartitionSet() {
        return partitionset;
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
                + ((partitionset == null) ? 0 : partitionset.hashCode());
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
        PartitionInvocationObserver other = (PartitionInvocationObserver) obj;
        if (cachename == null) {
            if (other.cachename != null) {
                return false;
            }
        } else if (!cachename.equals(other.cachename)) {
            return false;
        }
        if (partitionset == null) {
            if (other.partitionset != null) {
                return false;
            }
        } else if (!partitionset.equals(other.partitionset)) {
            return false;
        }
        return true;
    }
}