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

import java.util.HashSet;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.TransactionProcStatus;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.pof.SetCodec;
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
