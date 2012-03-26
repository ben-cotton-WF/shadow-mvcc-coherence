package com.sixwhits.cohmvcc.transaction;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;

import com.sixwhits.cohmvcc.cache.internal.MVCCNamedCache;
import com.sixwhits.cohmvcc.cache.internal.MVCCTransactionalCacheImpl;
import com.sixwhits.cohmvcc.domain.IsolationLevel;

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
    private final boolean readOnly;
    private final boolean autoCommit;
    private final IsolationLevel isolationLevel;
    private ThreadLocal<SessionTransactionManager> transactionManagers = new ThreadLocal<SessionTransactionManager>();

    /**
     * @param timestampSource source of timestamps
     */
    public ThreadTransactionManager(final TimestampSource timestampSource) {
        super();
        this.timestampSource = timestampSource;
        readOnly = false;
        autoCommit = false;
        isolationLevel = readCommitted;
    }

    /**
     * @param timestampSource source of timestamps
     * @param readOnly default read-only state for new transactions
     * @param autoCommit default auto-commit status for new transactions
     * @param isolationLevel default isolation level for new transaction
     */
    public ThreadTransactionManager(final TimestampSource timestampSource, 
            final boolean readOnly, final boolean autoCommit, final IsolationLevel isolationLevel) {
        super();
        this.timestampSource = timestampSource;
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
        this.isolationLevel = isolationLevel;
    }

    /**
     * @return the session transaction manager instance for the current thread.
     */
    private SessionTransactionManager getThreadTransactionManager() {
        if (transactionManagers.get() == null) {
            transactionManagers.set(
                    new SessionTransactionManager(timestampSource, readOnly, autoCommit, isolationLevel));
        }
        return transactionManagers.get();
    }
    
    /**
     * Get the invocation service name. Override to provide an alternate
     * @return the invocation service name.
     * TODO configuration option
     */
    protected String getInvocationServiceName() {
        return DEFAULT_INVOCATION_SERVICE_NAME;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public MVCCNamedCache getCache(final String cacheName) {
        getThreadTransactionManager().registerCache(cacheName);
        return new MVCCNamedCache(this, new MVCCTransactionalCacheImpl(cacheName, getInvocationServiceName()));
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
