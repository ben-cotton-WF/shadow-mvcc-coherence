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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
 * Filter to find the current versions from a cache. May be refined with either a set of keys
 * or a child filter.
 * 
 * @author David Whitmarsh from an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 * 
 * @param <K> the logical cache key
 */
@Portable
public class MVCCSurfaceFilter<K> implements IndexAwareFilter, Serializable {

    private static final long serialVersionUID = 5267677476884085089L;

    public static final int POF_TXID = 0;
    @PortableProperty(POF_TXID)
    private TransactionId transactionId;
    public static final int POF_KEYSET = 1;
    @PortableProperty(POF_KEYSET)
    private Collection<K> keySet;
    public static final int POF_FILTER = 2;
    @PortableProperty(POF_FILTER)
    private Filter filter = null;
    
    /**
     *  Default constructor for POF use only.
     */
    public MVCCSurfaceFilter() {
        // required 
    }
    
    /**
     * Constructor for the unqualified filter. This will gather all matching candidates.
     * 
     * @param transactionId the transaction id
     */
    public MVCCSurfaceFilter(final TransactionId transactionId) {
        super();
        this.transactionId = transactionId;
    }

    /**
     * Constructor for a filter qualified by a set of keys. Only candidates
     * contained in the keyset will match.
     * 
     * @param transactionId the transaction id
     * @param keySet set of logical cache keys
     */
    public MVCCSurfaceFilter(final TransactionId transactionId, final Collection<K> keySet) {
        super();
        this.transactionId = transactionId;
        this.keySet = Collections.unmodifiableCollection(keySet);
    }

    /**
     * Constructor for a filter qualified by an additional filter. Only
     * candidates matching the filter will be returned.
     * @param transactionId the transaction id
     * @param filter the filter
     */
    public MVCCSurfaceFilter(final TransactionId transactionId, final Filter filter) {
        super();
        this.transactionId = transactionId;
        this.filter = filter;
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
        Filter result = null;
        MVCCIndex<K> index = getIndex(indexes);
        if (keySet != null) {
            Set<Binary> floorBinaryKeys = new HashSet<Binary>(keySet.size());
            for (K key : keySet) {
                Binary keyFloor = index.floor(key, transactionId);
                if (keyFloor != null) {
                    floorBinaryKeys.add(keyFloor);
                }
            }
            candidates.retainAll(floorBinaryKeys);
        } else {
            if (filter != null && filter instanceof IndexAwareFilter) {
                result = ((IndexAwareFilter) filter).applyIndex(indexes, candidates);
            } else {
                result = filter;
            }
            candidates.retainAll(index.floorSet(candidates, transactionId));
        }
        
        // no further filtering required
        return result == null ? null : new FilterWrapper(result);
    }

    /**
     * @param indexes the indexes from the backing map context
     * @return the MVCC index
     */
    @SuppressWarnings("rawtypes")
    private MVCCIndex<K> getIndex(final Map indexes) {
        @SuppressWarnings("unchecked")
        MVCCIndex<K> result = (MVCCIndex<K>) indexes.get(new MVCCExtractor());
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
        MVCCSurfaceFilter<K> other = (MVCCSurfaceFilter<K>) obj;
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
