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

package com.shadowmvcc.coherence.domain;

import java.util.NavigableSet;

import com.shadowmvcc.coherence.pof.SortedSetCodec;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Wrapper for the set of read timestamps that are the
 * value object of a key cache, and the snapshot cache.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class TransactionSetWrapper {
    @PortableProperty(value = 0, codec = SortedSetCodec.class)
    private NavigableSet<TransactionId> transactionIdSet;

    /**
     * @return the set of read transaction ids
     */
    public NavigableSet<TransactionId> getTransactionIdSet() {
        return transactionIdSet;
    }

    /**
     * @param transactionIdSet the set of read transaction ids
     */
    public void setTransactionIdSet(final NavigableSet<TransactionId> transactionIdSet) {
        this.transactionIdSet = transactionIdSet;
    }

}
