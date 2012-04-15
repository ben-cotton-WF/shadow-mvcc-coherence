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

package com.shadowmvcc.coherence.event;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.net.cache.CacheEvent;
import com.tangosol.util.ObservableMap;

/**
 *
 * Extends {@code CacheEvent} to provide additional transaction context.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCCacheEvent extends CacheEvent {

    /**
     * Commit status of the entry after the event.
     */
    public enum CommitStatus { open, commit, rollback }
    private static final long serialVersionUID = -1015113502920893252L;
    private final CommitStatus commitStatus;
    private final TransactionId transactionId;

    /**
     * @param map the underlying version cache
     * @param nId logical event type - insert, update, delete
     * @param oKey logical key
     * @param oValueOld value of the previous version
     * @param oValueNew value of the current version
     * @param synthetic synthetic event
     * @param transactionId transaction id
     * @param commitStatus commit status
     */
    public MVCCCacheEvent(final ObservableMap map, final int nId, final Object oKey, 
            final Object oValueOld, final Object oValueNew, final boolean synthetic,
            final TransactionId transactionId, final CommitStatus commitStatus) {
        super(map, nId, oKey, oValueOld, oValueNew, synthetic);
        this.commitStatus = commitStatus;
        this.transactionId = transactionId;
    }

    /**
     * @return the transaction Id responsible for the change
     */
    public TransactionId getTransactionId() {
        return transactionId;
    }

    /**
     * @return the commit status of the event
     */
    public CommitStatus getCommitStatus() {
        return commitStatus;
    }



}
