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

package com.shadowmvcc.coherence.transaction.internal;

import java.util.Collection;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.transaction.Transaction;
import com.shadowmvcc.coherence.transaction.TransactionNotificationListener;
import com.tangosol.net.partition.PartitionSet;

/**
 * Read-only transaction.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ReadOnlyTransaction implements Transaction {

    private final TransactionId transactionId;
    private final IsolationLevel isolationLevel;
    private final TransactionNotificationListener notificationListener;


    /**
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     * @param notificationListener notification listener
     */
    public ReadOnlyTransaction(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, 
            final TransactionNotificationListener notificationListener) {
        super();
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.notificationListener = notificationListener;
    }

    @Override
    public TransactionId getTransactionId() {
        return transactionId;
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public boolean isAutoCommit() {
        return false;
    }

    @Override
    public void addKeyAffected(final CacheName cacheName, final Object key) {
        throw new UnsupportedOperationException("read only transaction");
    }

    @Override
    public void addKeySetAffected(final CacheName cacheName, final Collection<Object> keys) {
        throw new UnsupportedOperationException("read only transaction");
    }

    @Override
    public void setRollbackOnly() {
        throw new UnsupportedOperationException("read only transaction");
    }

    @Override
    public void commit() {
        notificationListener.transactionComplete(this);
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("read only transaction");
    }

    @Override
    public void addPartitionSetAffected(final CacheName cacheName,
            final PartitionSet partitionSet) {
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }
}
