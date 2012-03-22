package com.sixwhits.cohmvcc.transaction.internal;

import java.util.HashSet;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionProcStatus;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.pof.SetCodec;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationService;
import com.tangosol.net.NamedCache;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Commit or rollback a set of keys in a single cache and member.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class KeyTransactionInvocable implements Invocable {

    private static final long serialVersionUID = -8152431261311438494L;
    
    @PortableProperty(0) private TransactionId transactionId;
    @PortableProperty(1) private CacheName cacheName;
    @PortableProperty(value = 2, codec = SetCodec.class) private Set<Object> cacheKeys;
    @PortableProperty(3) private TransactionProcStatus transactionStatus;

    /**
     *  Default constructor for POF use only.
     */
    public KeyTransactionInvocable() {
        super();
    }

    /**
     * @param transactionId transaction id
     * @param cacheName name of the cache
     * @param cacheKeys set of keys
     * @param transactionStatus commit or rollback
     */
    public KeyTransactionInvocable(final TransactionId transactionId,
            final CacheName cacheName, final Set<Object> cacheKeys,
            final TransactionProcStatus transactionStatus) {
        super();
        this.transactionId = transactionId;
        this.cacheName = cacheName;
        this.cacheKeys = cacheKeys;
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
        
        NamedCache vcache = CacheFactory.getCache(cacheName.getVersionCacheName());
        Set<VersionedKey<Object>> vkeys = new HashSet<VersionedKey<Object>>();
        for (Object key : cacheKeys) {
            vkeys.add(new VersionedKey<Object>(key, transactionId));
        }
        vcache.invokeAll(vkeys, agent);
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
                + ((cacheKeys == null) ? 0 : cacheKeys.hashCode());
        result = prime * result
                + ((cacheName == null) ? 0 : cacheName.hashCode());
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
        KeyTransactionInvocable other = (KeyTransactionInvocable) obj;
        if (cacheKeys == null) {
            if (other.cacheKeys != null) {
                return false;
            }
        } else if (!cacheKeys.equals(other.cacheKeys)) {
            return false;
        }
        if (cacheName == null) {
            if (other.cacheName != null) {
                return false;
            }
        } else if (!cacheName.equals(other.cacheName)) {
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
