package com.sixwhits.cohmvcc.transaction.internal;

import java.util.Collection;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.transaction.Transaction;
import com.sixwhits.cohmvcc.transaction.TransactionNotificationListener;
import com.tangosol.util.Filter;

/**
 * Transaction implementation for an autocommit transaction.
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
    public int addFilterAffected(final CacheName cacheName, final Filter filter) {
        return 0;
    }

    @Override
    public void filterKeysAffected(final int invocationId, final Collection<?> keys) {
    }

    @Override
    public void filterPartitionsAffected(final int invocationId, 
            final Collection<Integer> keys) {
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

}
