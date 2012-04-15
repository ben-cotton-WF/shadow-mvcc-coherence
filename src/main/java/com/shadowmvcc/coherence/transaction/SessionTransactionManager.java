package com.shadowmvcc.coherence.transaction;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;

import com.shadowmvcc.coherence.cache.internal.MVCCNamedCache;
import com.shadowmvcc.coherence.cache.internal.MVCCTransactionalCacheImpl;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.transaction.internal.AutoCommitTransaction;
import com.shadowmvcc.coherence.transaction.internal.ManagerCacheImpl;
import com.shadowmvcc.coherence.transaction.internal.ReadOnlyTransaction;
import com.shadowmvcc.coherence.transaction.internal.TransactionCache;
import com.shadowmvcc.coherence.transaction.internal.TransactionCacheImpl;
import com.shadowmvcc.coherence.transaction.internal.TransactionImpl;

/**
 * Implementation of {@link TransactionManager} that provides a session-like interaction with caches.
 * All caches obtained from an instance of {@code SessionTransactionManager} share the same transaction, in particular, 
 * cache operations in different threads participate in the same transaction.
 * 
 * TODO is there a race condition if one thread is performing an operation at the same time as another
 * thread starts a commit or rollback?
 *
 * @see ThreadTransactionManager
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SessionTransactionManager implements TransactionManager, 
        TransactionNotificationListener {

    private final TimestampSource timestampSource;
    private final int managerId;
    private final TransactionCache transactionCache;
    private final ManagerCache managerCache;
    private volatile boolean readOnly = false;
    private volatile boolean autoCommit = false;
    private volatile IsolationLevel isolationLevel = readCommitted;
    private int offset = 0;
    private long lastTimestamp = 0L;

    private volatile Transaction currentTransaction = null;

    /**
     * @param timestampSource source of timestamps
     */
    public SessionTransactionManager(final TimestampSource timestampSource) {
        super();
        this.timestampSource = timestampSource;
        this.managerCache = getManagerCache();
        this.managerId = managerCache.getManagerId();
        this.transactionCache = getTransactionCache();
    }

    /**
     * @param timestampSource source of timestamps
     * @param readOnly true to create read-only transactions
     * @param autoCommit true to autocommit all cache operations
     * @param isolationLevel default transaction isolation level
     */
    public SessionTransactionManager(final TimestampSource timestampSource, 
            final boolean readOnly, 
            final boolean autoCommit, final IsolationLevel isolationLevel) {
        super();
        this.timestampSource = timestampSource;
        this.managerCache = getManagerCache();
        this.managerId = managerCache.getManagerId();
        this.transactionCache = getTransactionCache();
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
        this.isolationLevel = isolationLevel;
    }
    
    /**
     * Get the managerIdSource. Protected to allow override for
     * unit testing or alternate implementations.
     * @return the ManagerCache.
     */
    protected ManagerCache getManagerCache() {
        return new ManagerCacheImpl();
    }
    
    /**
     * Get the invocation service name. Override to provide an alternate
     * @return the invocation service name.
     * TODO configuration option
     */
    protected String getInvocationServiceName() {
        return DEFAULT_INVOCATION_SERVICE_NAME;
    }
    
    /**
     * Construct the transaction cache object. Override for unit test
     * or for alternate implementations
     * @return the transaction cache
     */
    protected TransactionCache getTransactionCache() {
        return new TransactionCacheImpl(getInvocationServiceName());
    }

    @Override
    public void transactionComplete(final Transaction transaction) {
        if (transaction != currentTransaction) {
            throw new TransactionException("notifying unknown transaction complete " + transaction);
        }
        currentTransaction = null;
    }
    
    /**
     * Register a cache with this transaction manager.
     * Used to ensure correct transaction completeion if this client
     * should fail with incomplete transactions.
     * @param cacheName name of the cache to register
     */
    public void registerCache(final String cacheName) {
        managerCache.registerCache(managerId, cacheName);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MVCCNamedCache getCache(final String cacheName) {
        registerCache(cacheName);
        return new MVCCNamedCache(this, new MVCCTransactionalCacheImpl(cacheName, getInvocationServiceName()));
    }

    @Override
    public synchronized Transaction getTransaction() {
        if (currentTransaction == null) {

            if (autoCommit) {
                currentTransaction = new AutoCommitTransaction(getNextId(), isolationLevel, this);
            } else if (readOnly) {
                currentTransaction = new ReadOnlyTransaction(getNextId(), isolationLevel, this);
            } else {
                currentTransaction = new TransactionImpl(getNextId(), isolationLevel, this, transactionCache);
            }
        }
        return currentTransaction;
    }

    /**
     * @return a new, unique transaction id
     */
    private TransactionId getNextId() {
        long timestamp = timestampSource.getTimestamp();
        if (timestamp == lastTimestamp) {
            offset++;
        } else {
            offset = 0;
        }
        lastTimestamp = timestamp;
        return new TransactionId(timestamp, managerId, offset);
    }

    @Override
    public boolean isTransactionOpen() {
        return currentTransaction != null;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void setReadOnly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public void setIsolationLevel(final IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }


}
