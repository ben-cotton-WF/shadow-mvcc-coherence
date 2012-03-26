package com.sixwhits.cohmvcc.transaction.internal;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionProcStatus;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationService;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.PartitionedFilter;

/**
 * Commit or rollback all entries for a cache in a set of partitions that belong to a particular
 * transaction.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class PartitionTransactionInvocable implements Invocable {

    private static final long serialVersionUID = -8152431261311438494L;
    
    @PortableProperty(0) private TransactionId transactionId;
    @PortableProperty(1) private CacheName cacheName;
    @PortableProperty(2) private PartitionSet partitionSet;
    @PortableProperty(3) private TransactionProcStatus transactionStatus;

    /**
     *  Default constructor for POF use only.
     */
    public PartitionTransactionInvocable() {
        super();
    }

    /**
     * @param transactionId transaction id
     * @param cacheName cache name
     * @param partitionSet partitions
     * @param transactionStatus commit or rollback
     */
    public PartitionTransactionInvocable(final TransactionId transactionId,
            final CacheName cacheName,
            final PartitionSet partitionSet, final TransactionProcStatus transactionStatus) {
        super();
        this.transactionId = transactionId;
        this.cacheName = cacheName;
        this.partitionSet = partitionSet;
        this.transactionStatus = transactionStatus;
    }


    @Override
    public void init(final InvocationService invocationservice) {
    }

    @Override
    public void run() {
        
        EntryProcessor agent;
        switch (transactionStatus) {
        case committing:
            agent = EntryCommitProcessor.INSTANCE;
            break;
        case rollingback:
            agent = EntryRollbackProcessor.INSTANCE;
            break;
        default:
            throw new IllegalArgumentException("invalid transaction status " + transactionStatus);    
        }
        

        Filter versionFilter = new EqualsFilter(Constants.TXEXTRACTOR, transactionId);
        NamedCache vcache = CacheFactory.getCache(cacheName.getVersionCacheName());
        Filter partFilter = new PartitionedFilter(versionFilter, partitionSet);
        vcache.invokeAll(partFilter, agent);

    }

    @Override
    public Object getResult() {
        return null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((cacheName == null) ? 0 : cacheName.hashCode());
        result = prime * result
                + ((partitionSet == null) ? 0 : partitionSet.hashCode());
        result = prime * result
                + ((transactionId == null) ? 0 : transactionId.hashCode());
        result = prime
                * result
                + ((transactionStatus == null) ? 0 : transactionStatus
                        .hashCode());
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
        PartitionTransactionInvocable other = (PartitionTransactionInvocable) obj;
        if (cacheName == null) {
            if (other.cacheName != null) {
                return false;
            }
        } else if (!cacheName.equals(other.cacheName)) {
            return false;
        }
        if (partitionSet == null) {
            if (other.partitionSet != null) {
                return false;
            }
        } else if (!partitionSet.equals(other.partitionSet)) {
            return false;
        }
        if (transactionId == null) {
            if (other.transactionId != null) {
                return false;
            }
        } else if (!transactionId.equals(other.transactionId)) {
            return false;
        }
        if (transactionStatus != other.transactionStatus) {
            return false;
        }
        return true;
    }

}
