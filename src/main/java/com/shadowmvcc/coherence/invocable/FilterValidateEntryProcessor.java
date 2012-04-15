package com.shadowmvcc.coherence.invocable;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readUncommitted;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.Utils;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;


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
    @PortableProperty(4)
    protected EntryProcessor delegate;

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
                return new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>) priorEntry.getKey());
            }
        }

        ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, priorEntry, cacheName);

        if (!confirmFilterMatch(childEntry)) {
            return null;
        }

        return new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>) priorEntry.getKey(), false, true);
    }

}
