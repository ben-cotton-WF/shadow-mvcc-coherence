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

package com.shadowmvcc.coherence.transaction.internal;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;
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

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.internal.UnconditionalPutProcessor;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.shadowmvcc.coherence.invocable.DecorationExtractorProcessor;
import com.shadowmvcc.coherence.invocable.MVCCEntryProcessorWrapper;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.InvocableMap.EntryProcessor;

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
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testFilterCommit() {
//        
//        transactionCache.beginTransaction(TX, readCommitted);
//        
//        for (int i = 0; i < 10; i++) {
//            put(testCacheName, TX, i, TESTVALUE);
//            assertFalse((Boolean) testCache.invoke(
//                    new VersionedKey<Integer>(i, TX), DecorationExtractorProcessor.COMMITTED_INSTANCE));
//        }
//        Map<CacheName, Set<Filter>> cacheFilterMap = new HashMap<CacheName, Set<Filter>>();
//        
//        Filter filter = new EqualsFilter(IdentityExtractor.INSTANCE, TESTVALUE);
//        cacheFilterMap.put(testCacheName, Collections.singleton(filter));
//        
//        transactionCache.commitTransaction(TX, Collections.EMPTY_MAP, cacheFilterMap);
//        
//        assertEquals(10, testCache.size());
//        
//        for (int i = 0; i < 10; i++) {
//            assertTrue((Boolean) testCache.invoke(
//                    new VersionedKey<Integer>(i, TX), DecorationExtractorProcessor.COMMITTED_INSTANCE));
//        }
//        
//    }
//    
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
