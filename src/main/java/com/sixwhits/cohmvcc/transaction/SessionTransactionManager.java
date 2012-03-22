package com.sixwhits.cohmvcc.transaction;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;

import com.sixwhits.cohmvcc.cache.internal.MVCCNamedCache;
import com.sixwhits.cohmvcc.cache.internal.MVCCTransactionalCacheImpl;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.transaction.internal.AutoCommitTransaction;
import com.sixwhits.cohmvcc.transaction.internal.ReadOnlyTransaction;
import com.sixwhits.cohmvcc.transaction.internal.TransactionActualScope;
import com.sixwhits.cohmvcc.transaction.internal.TransactionCache;
import com.sixwhits.cohmvcc.transaction.internal.TransactionImpl;

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
    private final String invocationServiceName;
    private final TransactionCache transactionCache;
    private volatile boolean readOnly = false;
    private volatile boolean autoCommit = false;
    private volatile IsolationLevel isolationLevel = readCommitted;
    private int offset = 0;
    private long lastTimestamp = 0L;

    private volatile Transaction currentTransaction = null;

    /**
     * @param timestampSource source of timestamps
     * @param managerIdSource source of transaction manager ids
     * @param invocationServiceName name of the invocation service to use
     * @param transactionCache DAO for accessing the transaction cache
     */
    public SessionTransactionManager(final TimestampSource timestampSource, 
            final ManagerIdSource managerIdSource, final String invocationServiceName,
            final TransactionCache transactionCache) {
        super();
        this.timestampSource = timestampSource;
        this.managerId = managerIdSource.getManagerId();
        this.invocationServiceName = invocationServiceName;
        this.transactionCache = transactionCache;
    }

    /**
     * @param timestampSource source of timestamps
     * @param managerIdSource source of transaction manager ids
     * @param invocationServiceName name of the invocation service to use
     * @param readOnly true to create read-only transactions
     * @param autoCommit true to autocommit all cache operations
     * @param isolationLevel default transaction isolation level
     * @param transactionCache DAO for accessing the transaction cache
     */
    public SessionTransactionManager(final TimestampSource timestampSource, 
            final ManagerIdSource managerIdSource, final String invocationServiceName,
            final TransactionCache transactionCache, final boolean readOnly, 
            final boolean autoCommit, final IsolationLevel isolationLevel) {
        super();
        this.timestampSource = timestampSource;
        this.managerId = managerIdSource.getManagerId();
        this.invocationServiceName = invocationServiceName;
        this.transactionCache = transactionCache;
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
        this.isolationLevel = isolationLevel;
    }

    @Override
    public void transactionComplete(final Transaction transaction) {
        if (transaction != currentTransaction) {
            throw new TransactionException("notifying unknown transaction complete " + transaction);
        }
        currentTransaction = null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MVCCNamedCache getCache(final String cacheName) {
        return new MVCCNamedCache(this, new MVCCTransactionalCacheImpl(cacheName, invocationServiceName));
    }

    @Override
    public synchronized Transaction getTransaction() {
        if (currentTransaction == null) {
            TransactionId transactionId = getNextId();
            TransactionActualScope tas = transactionCache.beginTransaction(transactionId, isolationLevel);
            
            if (tas.isReadonly()) {
                if (!this.readOnly) {
                    throw new TransactionException("Transaction with this timestamp must be read-only");
                }
                this.isolationLevel = tas.getIsolationLevel();
                transactionId = new TransactionId(tas.getTimestamp(), managerId, 0);
            }
            
            if (autoCommit) {
                currentTransaction = new AutoCommitTransaction(getNextId(), isolationLevel, this);
            } else if (readOnly) {
                currentTransaction = new ReadOnlyTransaction(getNextId(), isolationLevel, this);

            // TODO What about read-only & autocommit together?
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
