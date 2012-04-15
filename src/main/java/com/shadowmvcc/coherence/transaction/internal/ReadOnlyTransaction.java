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
}
