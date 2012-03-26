package com.sixwhits.cohmvcc.monitor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.TransactionCacheValue;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionProcStatus;
import com.sixwhits.cohmvcc.transaction.ManagerCache;
import com.sixwhits.cohmvcc.transaction.internal.EntryCommitProcessor;
import com.sixwhits.cohmvcc.transaction.internal.EntryRollbackProcessor;
import com.sixwhits.cohmvcc.transaction.internal.TransactionCache;
import com.sixwhits.cohmvcc.transaction.internal.TransactionStateUpdater;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Cluster;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Disposable;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
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
public class MemberTransactionMonitor implements Runnable, Disposable {

    private final int openTransactionTimeoutMillis;
    private final int transactionCompletionTimeoutMillis;
    private final int pollInterval;
    private final Semaphore shutdownFlag = new Semaphore(0);
    /**
     * Default maximum time between beginning a transaction and commit or rollback.
     */
    public static final int DEFAULT_OPEN_TRANSACTION_TIMEOUT_MILLIS = 60000;
    /**
     * Default maximum time from starting a commit or rollback before the monitor takes over to force completion.
     */
    public static final int DEFAULT_TRANSACTION_COMPLETION_TIMEOUT_MILLIS = 10000;
    /**
     * Default interval between polls of the transaction cache.
     */
    public static final int DEFAULT_POLL_INTERVAL_MILLIS = 5000;

    private static final PofExtractor TIMEEXTRACTOR = new PofExtractor(null, TransactionCacheValue.POF_REALTIME);
    private static final PofExtractor STATUSEXTRACTOR = new PofExtractor(null, TransactionCacheValue.POF_STATUS);
    private static final Filter OPENFILTER = new EqualsFilter(STATUSEXTRACTOR, TransactionProcStatus.open);
    private static final Filter ROLLBACKFILTER = new EqualsFilter(STATUSEXTRACTOR, TransactionProcStatus.rollingback);
    private static final Filter COMMITFILTER = new EqualsFilter(STATUSEXTRACTOR, TransactionProcStatus.committing);
    private static final EntryProcessor REMOVEPROCESSOR = new ConditionalRemove(AlwaysFilter.INSTANCE, false);
    
    /**
     * Constructor using user provided timeout values.
     * @param openTransactionTimeoutMillis timeout before an open transaction is rolled back
     * @param transactionCompletionTimeoutMillis timeout before a committing or rolling back transaction is completed
     * @param pollInterval how often to poll the transaction cache
     */
    public MemberTransactionMonitor(final int openTransactionTimeoutMillis,
            final int transactionCompletionTimeoutMillis, final int pollInterval) {
        super();
        this.openTransactionTimeoutMillis = openTransactionTimeoutMillis;
        this.transactionCompletionTimeoutMillis = transactionCompletionTimeoutMillis;
        this.pollInterval = pollInterval;
    }
    
    /**
     * Default constructor using the default timeout values. 
     */
    public MemberTransactionMonitor() {
        super();
        this.openTransactionTimeoutMillis = DEFAULT_OPEN_TRANSACTION_TIMEOUT_MILLIS;
        this.transactionCompletionTimeoutMillis = DEFAULT_TRANSACTION_COMPLETION_TIMEOUT_MILLIS;
        this.pollInterval = DEFAULT_POLL_INTERVAL_MILLIS;
    }

    @Override
    public void run() {

        if (!isStorageEnabled()) {
            return;
        }
        
        Cluster cluster = CacheFactory.getCluster();
        cluster.registerResource("memberTransactionMonitor:" + cluster.getLocalMember().getId(), this);
        
        do {
            try {
                if (shutdownFlag.tryAcquire(pollInterval, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
            }
            
            //TODO consider error conditions - die, carry on regardless?
            checkTransactions();
            
        } while (true);
        
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
     * Any that have been open too long are set to rollback, these together with any that have been
     * rolling back for too long have rollback completed, Transactions that have been in committing state
     * to long have the transaction commits completed. All completed transactions are then deleted.
     */
    private void checkTransactions() {

        NamedCache transactionCache = CacheFactory.getCache(TransactionCache.CACHENAME);
        
        DistributedCacheService cacheService = (DistributedCacheService) transactionCache.getCacheService();
        Member thisMember = CacheFactory.getCluster().getLocalMember();
        
        long now = System.currentTimeMillis();

        // set expired open transactions to rollback status, remembering the transaction ids
        PartitionSet memberParts = cacheService.getOwnedPartitions(thisMember);
        Filter openTimeout = new LessFilter(TIMEEXTRACTOR, now - openTransactionTimeoutMillis);
        Filter expiredOpenFilter = new AndFilter(OPENFILTER, openTimeout);
        Filter expiredOpenPartitionFilter = new PartitionedFilter(expiredOpenFilter, memberParts);
        
        @SuppressWarnings("unchecked")
        Set<TransactionId> expiredOpenTransactions = transactionCache.invokeAll(
                expiredOpenPartitionFilter, TransactionStateUpdater.ROLLBACK).keySet();
        
        Filter completeTimeout = new LessFilter(TIMEEXTRACTOR, now - transactionCompletionTimeoutMillis);

        Filter expiredRollbackFilter = new AndFilter(ROLLBACKFILTER, completeTimeout);
        Filter rollbackPartionFilter = new PartitionedFilter(expiredRollbackFilter, memberParts);
        
        @SuppressWarnings("unchecked")
        Set<TransactionId> expiredRollBacks = transactionCache.keySet(rollbackPartionFilter);
        
        expiredRollBacks.addAll(expiredOpenTransactions);

        Filter expiredCommitFilter = new AndFilter(COMMITFILTER, completeTimeout);
        Filter commitPartionFilter = new PartitionedFilter(expiredCommitFilter, memberParts);
        
        @SuppressWarnings("unchecked")
        Set<TransactionId> expiredCommits = transactionCache.keySet(commitPartionFilter);

        if (!expiredCommits.isEmpty()) {
            for (String cacheName : getCachesForTransactions(expiredCommits)) {
                NamedCache cache = CacheFactory.getCache(cacheName);
                Filter inTransactionsFilter = new InFilter(Constants.TXEXTRACTOR, expiredCommits);
                cache.invokeAll(inTransactionsFilter, EntryCommitProcessor.INSTANCE);
            }

            //TODO consider any failure conditions that we should deal with before doing this?
            transactionCache.invokeAll(expiredCommits, REMOVEPROCESSOR);
        }
        
        if (!expiredRollBacks.isEmpty()) {
            for (String cacheName : getCachesForTransactions(expiredRollBacks)) {
                NamedCache cache = CacheFactory.getCache(new CacheName(cacheName).getVersionCacheName());
                Filter inTransactionsFilter = new InFilter(Constants.TXEXTRACTOR, expiredRollBacks);
                cache.invokeAll(inTransactionsFilter, EntryRollbackProcessor.INSTANCE);
            }

            transactionCache.invokeAll(expiredRollBacks, REMOVEPROCESSOR);
        }
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
    public void dispose() {
        shutdownFlag.release();
    }

}
