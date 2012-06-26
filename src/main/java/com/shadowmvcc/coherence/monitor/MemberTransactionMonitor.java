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

package com.shadowmvcc.coherence.monitor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.config.Configuration;
import com.shadowmvcc.coherence.config.ConfigurationFactory;
import com.shadowmvcc.coherence.domain.Constants;
import com.shadowmvcc.coherence.domain.TransactionCacheValue;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.TransactionProcStatus;
import com.shadowmvcc.coherence.transaction.ManagerCache;
import com.shadowmvcc.coherence.transaction.internal.EntryCommitProcessor;
import com.shadowmvcc.coherence.transaction.internal.EntryRollbackProcessor;
import com.shadowmvcc.coherence.transaction.internal.TransactionCache;
import com.shadowmvcc.coherence.transaction.internal.TransactionStateUpdater;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Cluster;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Disposable;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.ServiceEvent;
import com.tangosol.util.ServiceListener;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.AndFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.InFilter;
import com.tangosol.util.filter.LessFilter;
import com.tangosol.util.filter.PartitionedFilter;
import com.tangosol.util.processor.ConditionalRemove;

/**
 * Monitor the ages of transaction entries in the local member. If any remain open
 * beyond the open transaction timeout value, force a rollback. If any remain in
 * committing or rolling back status longer than the completion timeout, then invoke
 * the commit/rollback completion process.
 * 
 * This thread should never normally terminate, so must be run as a daemon thread.
 * 
 * Any activity undertaken by this class may be interrupted when partially completed by a member
 * failure. The member that is assigned the partitions of the transaction cache should identify and
 * complete the outstanding transaction cleanup seamlessly
 * 
 * TODO graceful shutdown of this thread if the node is stopped or shutdown.
 * At the moment you can get spurious exceptions
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MemberTransactionMonitor implements Runnable, ServiceListener, Disposable {

    private final long openTransactionTimeoutMillis;
    private final long transactionCompletionTimeoutMillis;
    private final long pollInterval;
    private final Semaphore shutdownFlag = new Semaphore(0);
    private final Semaphore shutdownCompleteFlag = new Semaphore(0);

    private static final PofExtractor TIMEEXTRACTOR = new PofExtractor(null, TransactionCacheValue.POF_REALTIME);
    private static final PofExtractor STATUSEXTRACTOR = new PofExtractor(null, TransactionCacheValue.POF_STATUS);
    private static final Filter OPENFILTER = new EqualsFilter(STATUSEXTRACTOR, TransactionProcStatus.open);
    private static final Filter ROLLBACKFILTER = new EqualsFilter(STATUSEXTRACTOR, TransactionProcStatus.rollingback);
    private static final Filter COMMITFILTER = new EqualsFilter(STATUSEXTRACTOR, TransactionProcStatus.committing);
    private static final EntryProcessor REMOVEPROCESSOR = new ConditionalRemove(AlwaysFilter.INSTANCE, false);
    
  
    /**
     * Default constructor using the default timeout values. 
     */
    public MemberTransactionMonitor() {
        super();
        Configuration config = ConfigurationFactory.getConfiguraration();
        this.openTransactionTimeoutMillis = config.getOpenTransactionTimeout();
        this.transactionCompletionTimeoutMillis = config.getTransactionCompletionTimeout();
        this.pollInterval = config.getTransactionPollInterval();
    }

    @Override
    public void run() {

        if (!isStorageEnabled()) {
            return;
        }
        
        registerListener();
        
        do {
            try {
                if (shutdownFlag.tryAcquire(pollInterval, TimeUnit.MILLISECONDS)) {
                    shutdownCompleteFlag.release();
                    return;
                }
            } catch (InterruptedException e) {
            }
            
            //TODO consider error conditions - die, carry on regardless?
            if (checkTransactions()) {
                shutdownCompleteFlag.release();
                return;
            }
            
        } while (true);
        
    }
    
    /**
     * Register to catch service stopping and dispose events so as to shut down gracefully
     * before the member stops.
     */
    private void registerListener() {
        
        Cluster cluster = CacheFactory.getCluster();
        cluster.registerResource("memberTransactionMonitor:" + cluster.getLocalMember().getId(), this);
        
        NamedCache transactionCache = CacheFactory.getCache(TransactionCache.CACHENAME);
        
        DistributedCacheService cacheService = (DistributedCacheService) transactionCache.getCacheService();
        
        cacheService.addServiceListener(this);
        
    }
    
    /**
     * Check if local storage is enabled for the transaction cache.
     * @return true if storage is enabled
     */
    private boolean isStorageEnabled() {
        
        NamedCache transactionCache = CacheFactory.getCache(TransactionCache.CACHENAME);
        
        DistributedCacheService cacheService = (DistributedCacheService) transactionCache.getCacheService();
        
        return cacheService.isLocalStorageEnabled();
        
    }

    /**
     * Check all of the transactions in partitions of the transaction cache owned by this member.
     * Any that have been rolling back for too long have rollback completed, Transactions that have
     * been in committing state too long have the transaction commits completed. All completed
     * transactions are then deleted. Transactions that have been open too long are set to rollback,
     * these should then be picked up on the next poll to have rollback completed. The intervening
     * poll period allows any in-flight processing to be completed and client notified to prevent races
     * conditions. Between potentially time-consuming operations check the shutdown flag
     * @return true if a thread shutdown is required
     */
    private boolean checkTransactions() {

        try {
            NamedCache transactionCache = CacheFactory.getCache(TransactionCache.CACHENAME);

            DistributedCacheService cacheService = (DistributedCacheService) transactionCache.getCacheService();
            Member thisMember = CacheFactory.getCluster().getLocalMember();

            long now = System.currentTimeMillis();

            // set expired open transactions to rollback status, remembering the transaction ids
            PartitionSet memberParts = cacheService.getOwnedPartitions(thisMember);
            Filter openTimeout = new LessFilter(TIMEEXTRACTOR, now - openTransactionTimeoutMillis);
            Filter expiredOpenFilter = new AndFilter(OPENFILTER, openTimeout);
            Filter expiredOpenPartitionFilter = new PartitionedFilter(expiredOpenFilter, memberParts);

            transactionCache.invokeAll(
                    expiredOpenPartitionFilter, TransactionStateUpdater.ROLLBACK).keySet();

            if (shutdownFlag.tryAcquire()) {
                return true;
            }

            Filter completeTimeout = new LessFilter(TIMEEXTRACTOR, now - transactionCompletionTimeoutMillis);

            Filter expiredRollbackFilter = new AndFilter(ROLLBACKFILTER, completeTimeout);
            Filter rollbackPartionFilter = new PartitionedFilter(expiredRollbackFilter, memberParts);

            @SuppressWarnings("unchecked")
            Set<TransactionId> expiredRollBacks = transactionCache.keySet(rollbackPartionFilter);

            if (shutdownFlag.tryAcquire()) {
                return true;
            }

            Filter expiredCommitFilter = new AndFilter(COMMITFILTER, completeTimeout);
            Filter commitPartionFilter = new PartitionedFilter(expiredCommitFilter, memberParts);

            @SuppressWarnings("unchecked")
            Set<TransactionId> expiredCommits = transactionCache.keySet(commitPartionFilter);

            if (shutdownFlag.tryAcquire()) {
                return true;
            }

            if (!expiredCommits.isEmpty()) {
                for (String cacheName : getCachesForTransactions(expiredCommits)) {
                    NamedCache cache = CacheFactory.getCache(cacheName);
                    Filter inTransactionsFilter = new InFilter(Constants.TRANSACTIONIDEXTRACTOR, expiredCommits);
                    cache.invokeAll(inTransactionsFilter, EntryCommitProcessor.INSTANCE);

                    if (shutdownFlag.tryAcquire()) {
                        return true;
                    }
                }

                //TODO consider any failure conditions that we should deal with before doing this?
                transactionCache.invokeAll(expiredCommits, REMOVEPROCESSOR);
            }

            if (!expiredRollBacks.isEmpty()) {
                for (String cacheName : getCachesForTransactions(expiredRollBacks)) {
                    NamedCache cache = CacheFactory.getCache(new CacheName(cacheName).getVersionCacheName());
                    Filter inTransactionsFilter = new InFilter(Constants.TRANSACTIONIDEXTRACTOR, expiredRollBacks);
                    cache.invokeAll(inTransactionsFilter, EntryRollbackProcessor.INSTANCE);

                    if (shutdownFlag.tryAcquire()) {
                        return true;
                    }

                }

                transactionCache.invokeAll(expiredRollBacks, REMOVEPROCESSOR);
            }
        } catch (Throwable t) {
            if (shutdownFlag.tryAcquire()) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Get all of the caches that are affected by any of the transaction ids supplied.
     * The cache names are obtained by looking up the manager ids, and finding the
     * sets of caches registered with those manager.
     * 
     * @param transactionIds transaction ids to check
     * @return set of cache names.
     */
    private Set<String> getCachesForTransactions(final Set<TransactionId> transactionIds) {
        
        Set<String> result = new HashSet<String>();
        Set<Integer> managerIds = new HashSet<Integer>();
        for (TransactionId transactionId : transactionIds) {
            managerIds.add(transactionId.getContextId());
        }
        
        NamedCache managerCache = CacheFactory.getCache(ManagerCache.MGRCACHENAME);
        
        @SuppressWarnings("unchecked")
        Collection<Collection<String>> manangedCaches = managerCache.getAll(managerIds).values();
        for (Collection<String> cacheNames : manangedCaches) {
            result.addAll(cacheNames);
        }
        
        return result;
        
    }
    
    @Override
    public void serviceStarting(final ServiceEvent serviceevent) {
    }

    @Override
    public void serviceStarted(final ServiceEvent serviceevent) {
    }

    @Override
    public void serviceStopping(final ServiceEvent serviceevent) {
        shutdownFlag.release();
        try {
            shutdownCompleteFlag.acquire();
        } catch (InterruptedException e) {
        }
    }
    
    /**
     * explicitly stop the monitor.
     */
    public void stop() {
        shutdownFlag.release();
    }

    @Override
    public void serviceStopped(final ServiceEvent serviceevent) {
    }

    @Override
    public void dispose() {
        shutdownFlag.release();
    }

}
