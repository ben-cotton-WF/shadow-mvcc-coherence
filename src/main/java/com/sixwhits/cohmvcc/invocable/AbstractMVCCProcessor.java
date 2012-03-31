package com.sixwhits.cohmvcc.invocable;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionSetWrapper;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCIndex;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.filter.EntryFilter;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Abstract base class for {@code EntryProcessor} implementations that are invoked on
 * the key cache to manipulate or query related version cache entries.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> key class for this cache
 * @param <R> return type for the processor
 */
@Portable
public abstract class AbstractMVCCProcessor<K, R> extends AbstractProcessor {

    private static final long serialVersionUID = -8977457529050193716L;

    @PortableProperty(0) protected TransactionId transactionId;
    @PortableProperty(1) protected IsolationLevel isolationLevel;
    @PortableProperty(2) protected CacheName cacheName;
    @PortableProperty(3) protected Filter validationFilter = null;
    /**
     * Constructor.
     * @param transactionId the transaction Id
     * @param isolationLevel the isolation level
     * @param cacheName the cache name
     */
    public AbstractMVCCProcessor(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, final CacheName cacheName) {
        super();
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.cacheName = cacheName;
    }

    /**
     * Constructor.
     * @param transactionId the transaction Id
     * @param isolationLevel the isolation level
     * @param cacheName the cache name
     * @param validationFilter filter to apply to entry to confirm should be processed
     */
    public AbstractMVCCProcessor(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, final CacheName cacheName,
            final Filter validationFilter) {
        super();
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.cacheName = cacheName;
        this.validationFilter = validationFilter;
    }

    /**
     * Default constructor solely for POF use.
     */
    public AbstractMVCCProcessor() {
        super();
    }

    @Override
    public abstract ProcessorResult<K, R> process(Entry entryarg);

    /**
     * Get the set of read markers from the key cache entry.
     * @param entry the key cache entry
     * @return set of transactions.
     */
    protected NavigableSet<TransactionId> getReadTransactions(final Entry entry) {
        TransactionSetWrapper tsw = (TransactionSetWrapper) entry.getValue();
        return tsw == null ? null : tsw.getTransactionIdSet();
    }

    /**
     * Replace the read marker set.
     * @param entry the key cache entry
     * @param readTimestamps the set of read timestamps
     */
    protected void setReadTransactions(final Entry entry, final NavigableSet<TransactionId> readTimestamps) {
        TransactionSetWrapper tsw = new TransactionSetWrapper();
        tsw.setTransactionIdSet(readTimestamps);
        entry.setValue(tsw);
    }

    /**
     * Get the transaction id of the first future update.
     * @param entry the version cache entry
     * @return the transaction id of the future update, or null if there are none.
     */
    @SuppressWarnings("unchecked")
    protected TransactionId getNextWrite(final BinaryEntry entry) {
        MVCCIndex<K> index = (MVCCIndex<K>) entry.getBackingMapContext().getIndexMap().get(MVCCExtractor.INSTANCE);
        return index.ceilingTid((K) entry.getKey(), transactionId);
    }

    /**
     * Get the transaction id of the first future read.
     * @param entry the key cache entry
     * @return the transaction id or null if none found.
     */
    protected TransactionId getNextRead(final Entry entry) {
        NavigableSet<TransactionId> readTimestamps = getReadTransactions(entry);
        if (readTimestamps == null) {
            return null;
        }
        return readTimestamps.ceiling(transactionId);
    }

    /**
     * Get the backing map context for the version cache.
     * @param parentEntry key cache entry
     * @return the backing map context
     */
    protected BackingMapContext getVersionCacheBackingMapContext(final BinaryEntry parentEntry) {
        return parentEntry.getBackingMapContext().getManagerContext()
                .getBackingMapContext(cacheName.getVersionCacheName());
    }

    /**
     * get the binary key of the previous version from the version cache.
     * @param parentEntry key cache binary entry
     * @return binary key of the version cache, or null if none found
     */
    @SuppressWarnings("unchecked")
    protected Binary getPriorVersionBinaryKey(final BinaryEntry parentEntry) {

        MVCCIndex<K> index = (MVCCIndex<K>) getVersionCacheBackingMapContext(parentEntry)
                .getIndexMap().get(MVCCExtractor.INSTANCE);
        return index.floor((K) parentEntry.getKey(), transactionId);

    }

    /**
     * Set a read timestamp.
     * @param entry the key cache entry
     */
    protected void setReadTimestamp(final BinaryEntry entry) {
        NavigableSet<TransactionId> readTimestamps = getReadTransactions(entry);
        if (readTimestamps == null) {
            readTimestamps = new TreeSet<TransactionId>();
        }
        readTimestamps.add(transactionId);
        setReadTransactions(entry, readTimestamps);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Map processAll(final Set set) {
        // TODO delegate EntryProcessor that implement processAll() cannot be safely supported. Instead
        // provide an alternate interface to postprocess the resultmap.
        Map<K, ProcessorResult<K, R>> result = new HashMap<K, ProcessorResult<K, R>>();

        for (Entry entry : (Set<Entry>) set) {
            ProcessorResult<K, R> epr = process(entry);
            if (epr != null) {
                result.put((K) entry.getKey(), epr);
            }
        }
        
        return result;
    }

    /**
     * Check that a version cache entry matches the given validation filter.
     * @param childEntry a version cache entry
     * @return true if it matches
     */
    protected final boolean confirmFilterMatch(final Entry childEntry) {
        if (validationFilter != null) {
            if (validationFilter instanceof EntryFilter) {
                if (!((EntryFilter) validationFilter).evaluateEntry(childEntry)) {
                    return false;
                }
            } else if (!validationFilter.evaluate(childEntry.getValue())) {
                return false;
            }
        }
        return true;
    }
}