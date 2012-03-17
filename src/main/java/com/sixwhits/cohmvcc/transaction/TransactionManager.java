package com.sixwhits.cohmvcc.transaction;

import com.sixwhits.cohmvcc.cache.internal.MVCCNamedCache;
import com.sixwhits.cohmvcc.domain.IsolationLevel;

/**
 * {@code TransactionManager} implementations are responsible for creating {@link Transaction}
 * objects. Instances of {@link MVCCNamedCache} must be obtained from a {@code TransactionManager}
 * so that cache operations are performed withing the correct transaction.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TransactionManager {

    /**
     * Construct and return an {@link MVCCNamedCache}.
     * @param cacheName the cache name
     * @return the cache
     */
    MVCCNamedCache getCache(String cacheName);

    /**
     * Return the current transaction, constructs a new transaction if required.
     * @return the transaction
     */
    Transaction getTransaction();

    /**
     * Return true if a transaction has been started. Transactions are implicitly started by the
     * first cache operation, and removed after commit or rollback.
     * @return true if a transaction has been started.
     */
    boolean isTransactionOpen();

    /**
     * Set the isolation level for new transactions. Does not affect any transaction in progress.
     * @param isolationLevel the new isolation level.
     */
    void setIsolationLevel(IsolationLevel isolationLevel);

    /**
     * @return the current isolation level
     */
    IsolationLevel getIsolationLevel();

    /**
     * Set the  transaction manager in autocommit mode? If true, each cache operation creates
     * a new transaction, which is implicitly committed, Does not affect any currently open transaction
     * @param autoCommit true to set autocommit mode, false to clear
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * @return true if in autommit mode
     */
    boolean isAutoCommit();

    /**
     * Set transactions to read-only mode. Any update operation will fail. Does not affect
     * currently active transaction.
     * @param readOnly true to set read-only mode. False to clear
     */
    void setReadOnly(boolean readOnly);

    /**
     * @return true if the manager is in read only mode
     */
    boolean isReadOnly();

}
