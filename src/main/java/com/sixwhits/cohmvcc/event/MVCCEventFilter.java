package com.sixwhits.cohmvcc.event;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;
import static com.sixwhits.cohmvcc.domain.TransactionId.END_OF_TIME;

import java.util.Map.Entry;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCIndex;
import com.sixwhits.cohmvcc.index.MVCCIndex.IndexEntry;
import com.sixwhits.cohmvcc.invocable.ReadOnlyEntryWrapper;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.EntryFilter;

/**
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * Used to evaluate a filter in the context of a MapListener registration by filter, where
 * it is necessary to check both present and previous (in timestamp ordering) versions for a match
 * in order to detect changes causing an entry to leave a result set.
 *
 * @param <K> The key type of the logical cache
 */
@Portable
public class MVCCEventFilter<K> implements EntryFilter {

    public static final int POF_ISOLATION = 0;
    @PortableProperty(POF_ISOLATION)
    private IsolationLevel isolationLevel;
    public static final int POF_FILTER = 1;
    @PortableProperty(POF_FILTER)
    private Filter delegate;
    private static final int POF_CACHENAME = 2;
    @PortableProperty(POF_CACHENAME)
    private CacheName cacheName;
    
    @Override
    public boolean evaluate(final Object obj) {
        throw new UnsupportedOperationException("only supports binary entries");
    }

    @Override
    public boolean evaluateEntry(@SuppressWarnings("rawtypes") final Entry arg) {

        BinaryEntry entry = (BinaryEntry) arg;

        BinaryEntry wrappedEntry = new ReadOnlyEntryWrapper(entry, END_OF_TIME, isolationLevel, cacheName);

        boolean currentVersionMatch = match(wrappedEntry);

        if (currentVersionMatch) {
            return true;
        }

        @SuppressWarnings("unchecked")
        MVCCIndex<K> index = (MVCCIndex<K>) entry.getBackingMapContext().getIndexMap().get(MVCCExtractor.INSTANCE);
        @SuppressWarnings("unchecked")
        VersionedKey<K> currentVersion = (VersionedKey<K>) entry.getKey();

        Entry<TransactionId, IndexEntry> ixe = index.lowerEntry(
                currentVersion.getNativeKey(), currentVersion.getTxTimeStamp());
        while (ixe != null && (isolationLevel != readUncommitted || ixe.getValue().isCommitted())) {
            ixe = index.lowerEntry(currentVersion.getNativeKey(), ixe.getKey());
        }

        if (ixe != null) {
            Binary priorBinaryKey = ixe.getValue().getBinaryKey();
            Binary priorBinaryValue = (Binary) entry.getBackingMap().get(priorBinaryKey);

            @SuppressWarnings("unchecked")
            VersionedKey<K> priorKey = (VersionedKey<K>) ExternalizableHelper.fromBinary(
                    priorBinaryValue, entry.getSerializer());
            Binary logicalBinaryKey = ExternalizableHelper.toBinary(priorKey.getNativeKey());

            @SuppressWarnings("rawtypes")
            BinaryEntry priorEntry = new SyntheticBinaryEntry(logicalBinaryKey, priorBinaryValue, 
                    entry.getSerializer(), entry.getBackingMapContext());

            return match(priorEntry);

        }

        return false;

    }

    /**
     * Check if the entry matches the filter.
     * @param entry the entry
     * @return true if it matches
     */
    private boolean match(final BinaryEntry entry) {
        if (delegate instanceof EntryFilter) {
            return ((EntryFilter) delegate).evaluateEntry(entry);
        } else {
            return delegate.evaluate(entry.getValue());
        }
    }
}
