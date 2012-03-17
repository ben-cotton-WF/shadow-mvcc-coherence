package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.DeletedObject;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.Utils;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.exception.FutureReadException;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Wrapper class to execute an {@code EntryProcessor} written from the logical cache
 * perspective, against the key cache.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> Key type
 * @param <R> Return value type
 */
@Portable
public class MVCCEntryProcessorWrapper<K, R> extends AbstractMVCCProcessor<K, R> {

    private static final long serialVersionUID = -7158130705920331999L;

    public static final int POF_EP = 10;
    @PortableProperty(POF_EP)
    private EntryProcessor delegate;
    public static final int POF_AUTOCOMMIT = 11;
    @PortableProperty(POF_AUTOCOMMIT)
    private boolean autoCommit = false;

    /**
     * Default constructor for POF use only.
     */
    public MVCCEntryProcessorWrapper() {
    }

    /**
     * Constructor.
     * @param transactionId current transaction id
     * @param delegate EntryProcessor to execute
     * @param isolationLevel current isolation level
     * @param autoCommit implicit commit if true
     * @param cacheName cache name
     */
    public MVCCEntryProcessorWrapper(final TransactionId transactionId, 
            final EntryProcessor delegate, final IsolationLevel isolationLevel,
            final boolean autoCommit, final CacheName cacheName) {
        super(transactionId, isolationLevel, cacheName);
        this.delegate = delegate;
        this.autoCommit = autoCommit;
    }

    /**
     * Constructor.
     * @param transactionId current transaction id
     * @param delegate EntryProcessor to execute
     * @param isolationLevel current isolation level
     * @param autoCommit implicit commit if true
     * @param cacheName cache name
     * @param filter filter to confirm whether to process
     */
    public MVCCEntryProcessorWrapper(final TransactionId transactionId, 
            final EntryProcessor delegate, final IsolationLevel isolationLevel,
            final boolean autoCommit, final CacheName cacheName, final Filter filter) {
        super(transactionId, isolationLevel, cacheName, filter);
        this.delegate = delegate;
        this.autoCommit = autoCommit;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProcessorResult<K, R> process(final Entry entryarg) {

        BinaryEntry entry = (BinaryEntry) entryarg;

        if (isolationLevel != IsolationLevel.readUncommitted && isolationLevel != IsolationLevel.readProhibited) {
            Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
            if (priorVersionBinaryKey != null) {

                BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry)
                        .getBackingMapEntry(priorVersionBinaryKey);

                if (isolationLevel != IsolationLevel.readUncommitted) {
                    boolean committed = Utils.isCommitted(priorEntry);
                    if (!committed) {
                        return new ProcessorResult<K, R>(null, (VersionedKey<K>) priorEntry.getKey());
                    }
                }
            }
        }

        ReadWriteEntryWrapper childEntry = new ReadWriteEntryWrapper(entry, transactionId, isolationLevel, cacheName);

        if (!confirmFilterMatch(childEntry)) {
            return null;
        }

        R result = (R) delegate.process(childEntry);

        if (childEntry.isPriorRead() && isolationLevel == IsolationLevel.readProhibited) {
            throw new IllegalStateException("Read of prior version with isolation level readProhibited: "
                    + entry.getKey());
        }

        if (childEntry.isRemove() || childEntry.getNewBinaryValue() != null) {

            TransactionId nextRead = getNextRead(entry);
            if (nextRead != null) {
                TransactionId nextWrite = getNextWrite(childEntry);
                if (nextWrite == null || nextRead.compareTo(nextWrite) <= 0) {
                    throw new FutureReadException(new VersionedKey<K>((K) childEntry.getKey(), nextRead));
                }
            }

            Binary binaryKey = (Binary) childEntry.getContext().getKeyToInternalConverter().convert(
                    new VersionedKey<K>((K) childEntry.getKey(), transactionId));
            BinaryEntry newEntry = (BinaryEntry) childEntry.getBackingMapContext().getBackingMapEntry(binaryKey);

            Binary binaryValue;

            if (childEntry.isRemove()) {
                binaryValue = (Binary) childEntry.getContext()
                        .getValueToInternalConverter().convert(DeletedObject.INSTANCE);
            } else {
                binaryValue = childEntry.getNewBinaryValue();
            }

            binaryValue = Utils.decorateValue(binaryValue, autoCommit, childEntry.isRemove(), entry.getSerializer());

            newEntry.updateBinaryValue(binaryValue);
        }

        if ((isolationLevel == IsolationLevel.repeatableRead
                || isolationLevel == IsolationLevel.serializable) && childEntry.isPriorRead()) {
            setReadTimestamp(entry);
        }

        return new ProcessorResult<K, R>(result, null);
    }

}
