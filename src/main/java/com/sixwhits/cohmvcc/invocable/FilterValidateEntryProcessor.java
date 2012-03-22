package com.sixwhits.cohmvcc.invocable;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.Utils;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;


/**
 * EntryProcessor to evaluate a filter against an entry.
 * This will return a {@link ProcessorResult} with the
 * logical key value if the filter matches, null otherwise
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical key type
 */
@Portable
public class FilterValidateEntryProcessor<K> extends AbstractMVCCProcessor<K, VersionedKey<K>> {

    private static final long serialVersionUID = -954213053828163546L;

    /**
     * Default constructor for POF use only.
     */
    public FilterValidateEntryProcessor() {
        super();
    }

    /**
     * Constructor.
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     * @param cacheName cache name
     * @param validationFilter filter to execute
     */
    public FilterValidateEntryProcessor(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, final CacheName cacheName, 
            final Filter validationFilter) {
        super(transactionId, isolationLevel, cacheName, validationFilter);
    }


    @SuppressWarnings("unchecked")
    @Override
    public ProcessorResult<K, VersionedKey<K>> process(final Entry entryarg) {

        BinaryEntry entry = (BinaryEntry) entryarg;

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

        ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, priorEntry, cacheName);

        if (!confirmFilterMatch(childEntry)) {
            return null;
        }

        return new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>) priorEntry.getKey(), null);
    }

}
