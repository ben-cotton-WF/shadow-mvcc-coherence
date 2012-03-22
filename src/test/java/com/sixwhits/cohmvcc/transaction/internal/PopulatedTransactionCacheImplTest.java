package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.cache.internal.UnconditionalPutProcessor;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.invocable.DecorationExtractorProcessor;
import com.sixwhits.cohmvcc.invocable.MVCCEntryProcessorWrapper;
import com.sixwhits.cohmvcc.testsupport.AbstractLittlegridTest;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.filter.EqualsFilter;

/**
 * Test the transaction cache implementation for basic
 * commit/rollback and exception states using transactions with data.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class PopulatedTransactionCacheImplTest extends AbstractLittlegridTest {

    private CacheName testCacheName = new CacheName("testcache");
    private NamedCache testCache;
    private TransactionCache transactionCache;
    private static final TransactionId TX = new TransactionId(BASETIME, 0, 0);
    private static final String TESTVALUE = "a test value";
    
    /**
     * Set up the caches.
     */
    @Before
    public void setup() {
        transactionCache = new TransactionCacheImpl(INVOCATIONSERVICENAME);
        testCache = CacheFactory.getCache(testCacheName.getVersionCacheName());
        testCache.addIndex(MVCCExtractor.INSTANCE, false, null);
    }
    
    /**
     * Commit transaction, no baggage.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testKeyCommit() {
        
        transactionCache.beginTransaction(TX, readCommitted);
        
        Set<Object> txkeys = new HashSet<Object>();
        for (int i = 0; i < 10; i++) {
            put(testCacheName, TX, i, TESTVALUE);
            txkeys.add(i);
            assertFalse((Boolean) testCache.invoke(
                    new VersionedKey<Integer>(i, TX), DecorationExtractorProcessor.COMMITTED_INSTANCE));
        }
        Map<CacheName, Set<Object>> cacheKeyMap = new HashMap<CacheName, Set<Object>>();
        cacheKeyMap.put(testCacheName, txkeys);
        
        transactionCache.commitTransaction(TX, cacheKeyMap, Collections.EMPTY_MAP);
        
        assertEquals(10, testCache.size());
        
        for (int i = 0; i < 10; i++) {
            assertTrue((Boolean) testCache.invoke(
                    new VersionedKey<Integer>(i, TX), DecorationExtractorProcessor.COMMITTED_INSTANCE));
        }
        
    }
    
    /**
     * Commit transaction with filter, no baggage.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFilterCommit() {
        
        transactionCache.beginTransaction(TX, readCommitted);
        
        for (int i = 0; i < 10; i++) {
            put(testCacheName, TX, i, TESTVALUE);
            assertFalse((Boolean) testCache.invoke(
                    new VersionedKey<Integer>(i, TX), DecorationExtractorProcessor.COMMITTED_INSTANCE));
        }
        Map<CacheName, Set<Filter>> cacheFilterMap = new HashMap<CacheName, Set<Filter>>();
        
        Filter filter = new EqualsFilter(IdentityExtractor.INSTANCE, TESTVALUE);
        cacheFilterMap.put(testCacheName, Collections.singleton(filter));
        
        transactionCache.commitTransaction(TX, Collections.EMPTY_MAP, cacheFilterMap);
        
        assertEquals(10, testCache.size());
        
        for (int i = 0; i < 10; i++) {
            assertTrue((Boolean) testCache.invoke(
                    new VersionedKey<Integer>(i, TX), DecorationExtractorProcessor.COMMITTED_INSTANCE));
        }
        
    }
    
    /**
     * Rollback transaction, no baggage.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testKeyRollback() {
        
        transactionCache.beginTransaction(TX, readCommitted);
        
        Set<Object> txkeys = new HashSet<Object>();
        for (int i = 0; i < 10; i++) {
            put(testCacheName, TX, i, TESTVALUE);
            txkeys.add(i);
        }
        Map<CacheName, Set<Object>> cacheKeyMap = new HashMap<CacheName, Set<Object>>();
        cacheKeyMap.put(testCacheName, txkeys);
        
        transactionCache.rollbackTransaction(TX, cacheKeyMap, Collections.EMPTY_MAP);
        
        assertTrue(testCache.isEmpty());
        
    }
    
    /**
     * Utility function to put a value in uncommitted.
     * @param cacheName cache name
     * @param tid transaction id
     * @param key logical key
     * @param value value
     * @param <K> key type
     * @param <V> value type
     */
    private <K, V> void put(final CacheName cacheName, final TransactionId tid, final K key, final V value) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<K, V>(
                tid, new UnconditionalPutProcessor(value, true), readCommitted, false, cacheName);
        NamedCache keyCache = CacheFactory.getCache(cacheName.getKeyCacheName());
        keyCache.invoke(key, ep);
        
    }

}
