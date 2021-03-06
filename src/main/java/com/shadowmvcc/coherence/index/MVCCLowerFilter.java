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

package com.shadowmvcc.coherence.index;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.shadowmvcc.coherence.domain.TransactionId;
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
