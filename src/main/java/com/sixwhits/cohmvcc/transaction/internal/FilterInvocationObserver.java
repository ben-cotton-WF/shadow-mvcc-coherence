package com.sixwhits.cohmvcc.transaction.internal;

import java.util.Queue;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.invocable.InvocationObserverStatus;
import com.tangosol.net.Member;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;

/**
 * Observer for filter-based invocations.
 *
 */
class FilterInvocationObserver implements InvocationObserverStatus {
    
    private final PartitionSet partitionset;
    private final CacheName cachename;
    private final Set<Filter> filterSet;
    private final Queue<InvocationObserverStatus> resultQueue;
    private volatile boolean failed = false;

    /**
     * @param partitionset partition set invoked on
     * @param cachename cache name
     * @param filterSet filter set
     * @param resultQueue queue to place self on when complete
     */
    public FilterInvocationObserver(final PartitionSet partitionset, final CacheName cachename,
            final Set<Filter> filterSet, final Queue<InvocationObserverStatus> resultQueue) {
        super();
        this.partitionset = partitionset;
        this.cachename = cachename;
        this.filterSet = filterSet;
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
    /**
     * @return the filter set
     */
    public Set<Filter> getFilterSet() {
        return filterSet;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((cachename == null) ? 0 : cachename.hashCode());
        result = prime * result
                + ((filterSet == null) ? 0 : filterSet.hashCode());
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
        FilterInvocationObserver other = (FilterInvocationObserver) obj;
        if (cachename == null) {
            if (other.cachename != null) {
                return false;
            }
        } else if (!cachename.equals(other.cachename)) {
            return false;
        }
        if (filterSet == null) {
            if (other.filterSet != null) {
                return false;
            }
        } else if (!filterSet.equals(other.filterSet)) {
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