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

    @Override
    public TransactionId createSnapshot(final CacheName cacheName,
            final TransactionId snapshotId) {
        return getThreadTransactionManager().createSnapshot(cacheName, snapshotId);
    }

    @Override
    public void coalesceSnapshots(final CacheName cacheName,
            final TransactionId fromSnapshotId, final TransactionId toSnapshotId) {
        getThreadTransactionManager().coalesceSnapshots(cacheName, fromSnapshotId, toSnapshotId);
    }

}
