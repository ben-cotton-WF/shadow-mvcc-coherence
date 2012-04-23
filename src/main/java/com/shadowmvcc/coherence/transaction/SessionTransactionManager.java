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

package com.shadowmvcc.coherence.transaction;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.internal.MVCCNamedCache;
import com.shadowmvcc.coherence.cache.internal.MVCCTransactionalCacheImpl;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.transaction.internal.AutoCommitTransaction;
import com.shadowmvcc.coherence.transaction.internal.ManagerCacheImpl;
import com.shadowmvcc.coherence.transaction.internal.ReadOnlyTransaction;
import com.shadowmvcc.coherence.transaction.internal.SystemPropertyTimestampValidator;
import com.shadowmvcc.coherence.transaction.internal.TimestampValidator;
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
    private final TimestampValidator timestampValidator;
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
        this.timestampValidator = getTimestampValidator();
    }

    /**
     * Get the timestamp validator.
     * @return an instance of a TimestampValidator
     */
    protected TimestampValidator getTimestampValidator() {
        return new SystemPropertyTimestampValidator();
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
        this.timestampValidator = getTimestampValidator();
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
     * Used to ensure correct transaction completion if this client
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
    
    @SuppressWarnings("rawtypes")
    @Override
    public MVCCNamedCache getTemporalCacheView(final String cacheName, final long timestamp) {
        TransactionNotificationListener tnl = new TransactionNotificationListener() {
            @Override
            public void transactionComplete(final Transaction transaction) {
                throw new UnsupportedOperationException("Snapshot transaction");
            }
        };
        final Transaction viewTransaction = new ReadOnlyTransaction(
                new TransactionId(timestamp, Integer.MAX_VALUE, Integer.MAX_VALUE), isolationLevel, tnl);
        
        return new MVCCNamedCache(new TransactionManager() {
            @Override
            public MVCCNamedCache getCache(final String cacheName) {
                throw new UnsupportedOperationException();
            }
            @Override
            public MVCCNamedCache getTemporalCacheView(final String cacheName,
                    final long timestamp) {
                throw new UnsupportedOperationException();
            }
            @Override
            public Transaction getTransaction() {
                return viewTransaction;
            }

            @Override
            public TransactionId createSnapshot(final CacheName cacheName,
                    final long snapshotTime) {
                throw new UnsupportedOperationException("not yet implemented");
            }

            @Override
            public void coalesceSnapshots(final CacheName cacheName,
                    final long fromSnapshotTime, final long toSnapshotTime) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void coalesceSnapshots(final CacheName cacheName,
                    final long toSnapshotTime) {
                throw new UnsupportedOperationException();
            }
            
        }, new MVCCTransactionalCacheImpl(cacheName, getInvocationServiceName()));
    }

    @Override
    public synchronized Transaction getTransaction() {
        if (currentTransaction == null) {

            TransactionId id = getNextId();
            if (autoCommit) {
                if (!timestampValidator.isTransactionTimestampValid(id.getTimeStampMillis())) {
                    throw new TransactionException("Cannot create transaction, timestamp too old");
                }
                currentTransaction = new AutoCommitTransaction(id, isolationLevel, this);
            } else if (readOnly) {
                // Not a problem if read-only transactions are old.
                currentTransaction = new ReadOnlyTransaction(id, isolationLevel, this);
            } else {
                if (!timestampValidator.isTransactionTimestampValid(id.getTimeStampMillis())) {
                    throw new TransactionException("Cannot create transaction, timestamp too old");
                }
                currentTransaction = new TransactionImpl(id, isolationLevel, this, transactionCache);
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
        // TODO verify later than most recent snapshot 
        return new TransactionId(timestamp, managerId, offset);
    }

    @Override
    public TransactionId createSnapshot(final CacheName cacheName,
            final long snapshotTime) {
        // TODO verify no open transactions at or earlier than snapshotId
        TransactionId snapshotId = new TransactionId(snapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        return managerCache.createSnapshot(cacheName, snapshotId);
    }

    @Override
    public void coalesceSnapshots(final CacheName cacheName,
            final long fromSnapshotTime, final long toSnapshotTime) {
        TransactionId fromSnapshotId = new TransactionId(fromSnapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TransactionId toSnapshotId = new TransactionId(toSnapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        managerCache.coalesceSnapshots(cacheName, fromSnapshotId, toSnapshotId);
    }

    @Override
    public void coalesceSnapshots(final CacheName cacheName,
            final long toSnapshotTime) {
        TransactionId toSnapshotId = new TransactionId(toSnapshotTime, Integer.MAX_VALUE, Integer.MAX_VALUE);
        managerCache.coalesceSnapshots(cacheName, null, toSnapshotId);
    }


}
