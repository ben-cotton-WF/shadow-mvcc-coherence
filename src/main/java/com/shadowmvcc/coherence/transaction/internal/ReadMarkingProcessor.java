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

import static com.shadowmvcc.coherence.domain.IsolationLevel.repeatableRead;
import static com.shadowmvcc.coherence.domain.IsolationLevel.serializable;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.Utils;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.invocable.AbstractMVCCProcessor;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Entry processor to create a read marker in the key cache at a given timestamp.
 * By default returns null if successful and a {@link ProcessorResult} if it fails because of 
 * an uncommitted change. May optionally return a {@code ProcessorResult} indicating the key
 * updated
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical key type
 */
@Portable
public class ReadMarkingProcessor<K> extends AbstractMVCCProcessor<K, VersionedKey<K>> {

    private static final long serialVersionUID = -6559372127281694088L;

    public static final int POF_RETURNKEYS = 0;
    @PortableProperty(POF_RETURNKEYS)
    private boolean returnMatchingKeys = false;

    @PortableProperty(4)
    protected EntryProcessor delegate;
    
    /**
     * Default constructor for POF use only.
     */
    public ReadMarkingProcessor() {
        super();
    }

    /**
     * Constructor.
     * @param transactionId the transaction id
     * @param isolationLevel the isolation level
     * @param cacheName the cache name
     */
    public ReadMarkingProcessor(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, final CacheName cacheName) {
        super(transactionId, isolationLevel, cacheName);
    }

    /**
     * Constructor.
     * @param transactionId the transaction id
     * @param isolationLevel the isolation level
     * @param cacheName the cache name
     * @param returnMatchingKeys true to always return a {@code ProcessorResult}
     */
    public ReadMarkingProcessor(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, final CacheName cacheName, 
            final boolean returnMatchingKeys) {
        super(transactionId, isolationLevel, cacheName);
        this.returnMatchingKeys = returnMatchingKeys;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProcessorResult<K, VersionedKey<K>> process(final Entry arg) {
        BinaryEntry entry = (BinaryEntry) arg;
        Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
        if (priorVersionBinaryKey == null) {
            return null;
        }

        BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry)
                .getBackingMapEntry(priorVersionBinaryKey);

        if (isolationLevel != IsolationLevel.readUncommitted) {
            boolean committed = Utils.isCommitted(priorEntry);
            VersionedKey<K> priorKey = (VersionedKey<K>) priorEntry.getKey();
            if (!committed && !priorKey.getTransactionId().equals(transactionId)) {
                return new ProcessorResult<K, VersionedKey<K>>(cacheName, (VersionedKey<K>) priorEntry.getKey());
            }
        }

        boolean deleted = Utils.isDeleted(priorEntry);
        if (deleted) {
            return null;
        }

        if (isolationLevel == repeatableRead || isolationLevel == serializable) {
            setReadTimestamp(entry);
        }

        return returnMatchingKeys
                ? new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>) priorEntry.getKey(), null, true) : null;
    }
}
