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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;

/**
 * Filter to find all but the latest timestamp for each entry
 * within a range. Used to identify those versions that can be
 * purged when creating or coalescing snapshots. The range given is
 * exclusive at the start and inclusive end. i.e. an entry with the timestamp
 * at the range end time is included.
 * 
 * @author David Whitmarsh
 * 
 * @param <K> the logical cache key type
 */
@Portable
public class MVCCSnapshotPurgeFilter<K> implements IndexAwareFilter, Serializable {

    private static final long serialVersionUID = 5267677476884085089L;

    @PortableProperty(0) private TransactionId rangeStart;
    @PortableProperty(1) private TransactionId rangeEnd;
    
    /**
     *  Default constructor for POF use only.
     */
    public MVCCSnapshotPurgeFilter() {
        // required 
    }
    
    /**
     * @param rangeStart transaction id of start of range (exclusive)
     * @param rangeEnd transaction id of end of range (inclusive)
     */
    public MVCCSnapshotPurgeFilter(final TransactionId rangeStart, final TransactionId rangeEnd) {
        super();
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
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
        candidates.retainAll(index.snapshotPurgeSet(rangeStart, rangeEnd));

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

}
