package com.sixwhits.cohmvcc.index;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;

/**
 * Filter to find the set of entries with timestamp next lower than given.
 * 
 * @author David Whitmarsh
 * 
 * @param <K> the logical cache key type
 */
@Portable
public class MVCCLowerFilter<K> implements IndexAwareFilter, Serializable {

    private static final long serialVersionUID = 5267677476884085089L;

    public static final int POF_TXID = 0;
    @PortableProperty(POF_TXID)
    private TransactionId transactionId;
    public static final int POF_KEY = 1;
    @PortableProperty(POF_KEY)
    private K key;
    
    /**
     *  Default constructor for POF use only.
     */
    public MVCCLowerFilter() {
        // required 
    }
    
    /**
     * @param transactionId transaction id
     * @param key logical key
     */
    public MVCCLowerFilter(final TransactionId transactionId, final K key) {
        super();
        this.transactionId = transactionId;
        this.key = key;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean evaluateEntry(final Entry paramEntry) {
        throw new UnsupportedOperationException("Cannot work without index");
    }

    @Override
    public boolean evaluate(final Object paramObject) {
        throw new UnsupportedOperationException("Cannot work without index");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int calculateEffectiveness(final Map mapIndex, final Set candidates) {
        // cannot work without index anyway
        return 1;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Filter applyIndex(final Map indexes, final Set candidates) {
        MVCCIndex<K> index = getIndex(indexes);
        Binary keyLower = index.lower(key, transactionId);
        if (keyLower != null) {
            candidates.retainAll(Collections.singleton(keyLower));
        } else {
            candidates.clear();
        }
        
        // no further filtering required
        return null;
    }

    /**
     * Get the MVCC index.
     * @param indexes the indexes from the backing map context
     * @return the index
     */
    @SuppressWarnings("rawtypes")
    private MVCCIndex<K> getIndex(final Map indexes) {
        @SuppressWarnings("unchecked")
        MVCCIndex<K> result = (MVCCIndex<K>) indexes.get(MVCCExtractor.INSTANCE);
        if (result == null) {
            throw new IllegalArgumentException("No MVCCIndex defined");
        }
        return result;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((transactionId == null) ? 0 : transactionId.hashCode());
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
        @SuppressWarnings("unchecked")
        MVCCLowerFilter<K> other = (MVCCLowerFilter<K>) obj;
        if (transactionId == null) {
            if (other.transactionId != null) {
                return false;
            }
        } else if (!transactionId.equals(other.transactionId)) {
            return false;
        }
        return true;
    }
}
