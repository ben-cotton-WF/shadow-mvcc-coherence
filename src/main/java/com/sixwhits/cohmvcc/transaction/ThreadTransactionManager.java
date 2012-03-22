package com.sixwhits.cohmvcc.transaction;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;

import com.sixwhits.cohmvcc.cache.internal.MVCCNamedCache;
import com.sixwhits.cohmvcc.cache.internal.MVCCTransactionalCacheImpl;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.transaction.internal.TransactionCache;

/**
 * Implementation of {@TransactionManager} to provide a separate transaction context per thread.
 * Operations from two different threads on a cache obtained from this class will be in separate transactions.
 * Calls to set readonly, autocommit, and isolation level will affect only the calling thread.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ThreadTransactionManager implements TransactionManager {

    private final TimestampSource timestampSource;
    private final ManagerIdSource managerIdSource;
    private final String invocationServiceName;
    private final TransactionCache transactionCache;
    private final boolean readOnly;
    private final boolean autoCommit;
    private final IsolationLevel isolationLevel;
    private ThreadLocal<TransactionManager> transactionManagers = new ThreadLocal<TransactionManager>();

    /**
     * @param timestampSource source of timestamps
     * @param managerIdSource source of manager id
     * @param invocationServiceName name of service to perform invocations
     * @param transactionCache DAO for accessing the transaction cache
     */
    public ThreadTransactionManager(final TimestampSource timestampSource, 
            final ManagerIdSource managerIdSource, final String invocationServiceName,
            final TransactionCache transactionCache) {
        super();
        this.timestampSource = timestampSource;
        this.managerIdSource = managerIdSource;
        this.invocationServiceName = invocationServiceName;
        this.transactionCache = transactionCache;
        readOnly = false;
        autoCommit = false;
        isolationLevel = readCommitted;
    }

    /**
     * @param timestampSource source of timestamps
     * @param managerIdSource source of manager id
     * @param invocationServiceName name of service to perform invocations
     * @param transactionCache DAO for accessing the transaction cache
     * @param readOnly default read-only state for new transactions
     * @param autoCommit default auto-commit status for new transactions
     * @param isolationLevel default isolation level for new transaction
     */
    public ThreadTransactionManager(final TimestampSource timestampSource, 
            final ManagerIdSource managerIdSource, final String invocationServiceName,
            final TransactionCache transactionCache,
            final boolean readOnly, final boolean autoCommit, final IsolationLevel isolationLevel) {
        super();
        this.timestampSource = timestampSource;
        this.managerIdSource = managerIdSource;
        this.invocationServiceName = invocationServiceName;
        this.transactionCache = transactionCache;
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
        this.isolationLevel = isolationLevel;
    }

    /**
     * @return the session transaction manager instance for the current thread.
     */
    private TransactionManager getThreadTransactionManager() {
        if (transactionManagers.get() == null) {
            transactionManagers.set(
                    new SessionTransactionManager(timestampSource, managerIdSource,
                            invocationServiceName, transactionCache, readOnly, autoCommit, isolationLevel));
        }
        return transactionManagers.get();
    }
    @SuppressWarnings("rawtypes")
    @Override
    public MVCCNamedCache getCache(final String cacheName) {
        return new MVCCNamedCache(this, new MVCCTransactionalCacheImpl(cacheName, invocationServiceName));
    }

    @Override
    public Transaction getTransaction() {
        return getThreadTransactionManager().getTransaction();
    }

    @Override
    public boolean isTransactionOpen() {
        return getThreadTransactionManager().isTransactionOpen();
    }

    @Override
    public void setIsolationLevel(final IsolationLevel isolationLevel) {
        getThreadTransactionManager().setIsolationLevel(isolationLevel);
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return getThreadTransactionManager().getIsolationLevel();
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) {
        getThreadTransactionManager().setAutoCommit(autoCommit);
    }

    @Override
    public boolean isAutoCommit() {
        return getThreadTransactionManager().isAutoCommit();
    }

    @Override
    public void setReadOnly(final boolean readOnly) {
        getThreadTransactionManager().setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() {
        return getThreadTransactionManager().isReadOnly();
    }

}
