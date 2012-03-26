package com.sixwhits.cohmvcc.transaction.internal;

import java.util.Collection;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.transaction.Transaction;
import com.sixwhits.cohmvcc.transaction.TransactionNotificationListener;
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

}
