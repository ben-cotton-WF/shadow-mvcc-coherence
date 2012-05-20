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
import java.util.Collections;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.transaction.Transaction;
import com.shadowmvcc.coherence.transaction.TransactionNotificationListener;
import com.tangosol.net.partition.PartitionSet;

/**
 * Transaction implementation for an autocommit transaction. Most methods are empty stubs as all cache
 * updates are applied as already committed. When the {@code MVCCNamedCache} obtains the id from an autocommit
 * transaction, the transaction is marked as complete so that the next request to the {@code TransactionManager}
 * will obtain a new transaction
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class AutoCommitTransaction implements Transaction {

    private final TransactionId transactionId;
    private final IsolationLevel isolationLevel;
    private final TransactionNotificationListener notificationListener;


    /**
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     * @param notificationListener notification listener
     */
    public AutoCommitTransaction(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, 
            final TransactionNotificationListener notificationListener) {
        super();
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.notificationListener = notificationListener;
    }

    @Override
    public TransactionId getTransactionId() {
        notificationListener.transactionComplete(this);
        return transactionId;
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public boolean isAutoCommit() {
        return true;
    }

    @Override
    public void addKeyAffected(final CacheName cacheName, final Object key) {
    }

    @Override
    public void addKeySetAffected(final CacheName cacheName, final Collection<Object> keys) {
    }

    @Override
    public void setRollbackOnly() {
    }

    @Override
    public void commit() {
    }

    @Override
    public void rollback() {
    }

    @Override
    public void addPartitionSetAffected(final CacheName cacheName,
            final PartitionSet partitionSet) {
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setBlanketRollbackRequired(final CacheName cacheName,
            final boolean blanketRollbackRequired) {
    }

    @Override
    public Collection<CacheName> isBlanketRollbackRequired() {
        return Collections.emptyList();
    }

}
