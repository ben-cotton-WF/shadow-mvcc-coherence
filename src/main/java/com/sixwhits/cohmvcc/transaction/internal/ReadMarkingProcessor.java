package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.repeatableRead;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.serializable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.Utils;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.invocable.AbstractMVCCProcessor;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;

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

        if (isolationLevel != readUncommitted) {
            boolean committed = Utils.isCommitted(priorEntry);
            if (!committed) {
                return new ProcessorResult<K, VersionedKey<K>>(null, (VersionedKey<K>) priorEntry.getKey());
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
                ? new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>) priorEntry.getKey(), null) : null;
    }
}
