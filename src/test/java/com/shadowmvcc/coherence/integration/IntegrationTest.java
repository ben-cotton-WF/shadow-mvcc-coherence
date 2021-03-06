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

package com.shadowmvcc.coherence.integration;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.shadowmvcc.coherence.testsupport.OffsetableTimestampSource;
import com.shadowmvcc.coherence.transaction.SystemTimestampSource;
import com.shadowmvcc.coherence.transaction.ThreadTransactionManager;
import com.shadowmvcc.coherence.transaction.TimestampSource;
import com.shadowmvcc.coherence.transaction.TransactionException;
import com.shadowmvcc.coherence.transaction.TransactionManager;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.ContinuousQueryCache;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.processor.ConditionalPut;

/**
 * Simple integration test to pull all the components together.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class IntegrationTest extends AbstractLittlegridTest {
    
    private TransactionManager transactionManager;
    
    /**
     * initialise system properties.
     */
    @BeforeClass
    public static void setMonitorProperties() {
        System.setProperty("shadowmvcc.opentransactiontimeout", "1000");
        System.setProperty("shadowmvcc.transactioncompletiontimeout", "1000");
        System.setProperty("shadowmvcc.transactionpollinterval", "100");
    }
    /**
     * Set up the tm.
     */
    @Before
    public void initialiseTransactionManager() {
        
        TimestampSource timestampSource = new SystemTimestampSource();
        
        transactionManager = new ThreadTransactionManager(
                timestampSource, false, false, readCommitted);
        
    }
    
    /**
     * Get the cache and run a couple of transactions.
     */
    @Test
    public void testCompleteTransaction() {
        
        NamedCache cache = transactionManager.getCache("test-cache1");
        
        cache.put(1, "version 1");
        
        transactionManager.getTransaction().commit();
        
        cache.put(1, "version 2");

        transactionManager.getTransaction().commit();
        
        CacheName cacheName = new CacheName(cache.getCacheName()); 
        
        Assert.assertEquals(2, CacheFactory.getCache(cacheName.getVersionCacheName()).size());
    }
    
    /**
     * Test that an uncommitted transaction gets unwound by timeout.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testRollbackIncompleteTransaction() throws InterruptedException {

        CacheName cachename = new CacheName("test-cache1");
        NamedCache cache = transactionManager.getCache(cachename.getLogicalName());
        
        cache.put(1, "version 1");
        
        NamedCache vcache = CacheFactory.getCache(cachename.getVersionCacheName());

        Assert.assertEquals(1, vcache.size());

        Thread.sleep(2500);
        
        Assert.assertEquals(0, vcache.size());

    }
    
    /**
     * Test that an update fails after the transaction times out.
     * @throws InterruptedException if interrupted
     */
    @Test(expected = TransactionException.class)
    public void testUpdateExpiredTransaction() throws InterruptedException {

        CacheName cachename = new CacheName("test-cache1");
        NamedCache cache = transactionManager.getCache(cachename.getLogicalName());
        
        cache.put(1, "version 1");
        
        NamedCache vcache = CacheFactory.getCache(cachename.getVersionCacheName());

        Assert.assertEquals(1, vcache.size());

        Thread.sleep(1500);
        
        cache.put(1, "version 1");

    }
    
    /**
     * Test that an update fails after the transaction times out.
     * @throws InterruptedException if interrupted
     */
    @Test(expected = TransactionException.class)
    public void testExpireWhileWaitForUncommitted() throws InterruptedException {
        
        CacheName cachename = new CacheName("test-cache1");
        NamedCache cache1 = transactionManager.getCache(cachename.getLogicalName());

        try {
            OffsetableTimestampSource ts2 = new OffsetableTimestampSource(new SystemTimestampSource());
            ts2.setOffset(-1000L);

            TransactionManager tm2 = new ThreadTransactionManager(
                    ts2, false, false, readCommitted);


            NamedCache cache2 = tm2.getCache(cachename.getLogicalName());

            cache1.put(1, "version 1");

            Thread.sleep(800);

            cache2.put(2, "backdated transaction");
        } catch (TransactionException ex) {
            Assert.fail("unexpected transaction exception");
        }
        
        cache1.get(2);
        
    }
    
    /**
     * Finding the entries to rollback based on a filter may break if the
     * update causes the row to no longer match the filter.
     * Now fixed as we no longer record the filter for this
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testUpdateCausesFilterToNoLongerMatch() throws InterruptedException {
        
        CacheName cachename = new CacheName("test-cache1");
        NamedCache cache = transactionManager.getCache(cachename.getLogicalName());
        
        cache.put(1, "A");
        
        transactionManager.getTransaction().commit();
        
        Filter afilter = new EqualsFilter(IdentityExtractor.INSTANCE, "A");
        EntryProcessor putb = new ConditionalPut(afilter, "B");
        cache.invokeAll(afilter, putb);
        
        NamedCache vcache = CacheFactory.getCache(cachename.getVersionCacheName());

        Assert.assertEquals(2, vcache.size());
        
        transactionManager.getTransaction().rollback();
        
        // only the original version
        Assert.assertEquals(1, vcache.size());
    }
    
    /**
     * Test a CQC.
     * @throws InterruptedException never
     */
    @Test
    public void testCQC() throws InterruptedException {
        
        CacheName cachename = new CacheName("test-cache1");
        NamedCache cache = transactionManager.getCache(cachename.getLogicalName());
        
        final Map<Integer, MapEvent> capturedEvents = new HashMap<Integer, MapEvent>();
        
        MapListener listener = new MapListener() {
            
            @Override
            public void entryUpdated(final MapEvent evt) {
                capturedEvents.put((Integer) evt.getKey(), evt);
            }
            
            @Override
            public void entryInserted(final MapEvent evt) {
                capturedEvents.put((Integer) evt.getKey(), evt);
            }
            
            @Override
            public void entryDeleted(final MapEvent evt) {
                capturedEvents.put((Integer) evt.getKey(), evt);
            }
        };
        
        final String testValue1 = "testValue1";
        final String testValue2 = "testValue2";
        
        for (int i = 0; i < 10; i++) {
            cache.put(i, testValue1);
        }
        
        transactionManager.getTransaction().commit();
        
        @SuppressWarnings("unused")
        ContinuousQueryCache cqc = new ContinuousQueryCache(cache, AlwaysFilter.INSTANCE, listener);
        
        Thread.sleep(500);
        
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(testValue1, capturedEvents.get(i).getNewValue());
            Assert.assertEquals(MapEvent.ENTRY_INSERTED, capturedEvents.get(i).getId());
        }
        
        cache.remove(1);
        cache.put(2, testValue2);
        cache.put(100, testValue2);
        
        transactionManager.getTransaction().commit();
        
        Thread.sleep(500);

        assertEquals(MapEvent.ENTRY_DELETED, capturedEvents.get(1).getId());
        assertEquals(MapEvent.ENTRY_UPDATED, capturedEvents.get(2).getId());
        assertEquals(testValue2, capturedEvents.get(2).getNewValue());
        assertEquals(MapEvent.ENTRY_INSERTED, capturedEvents.get(100).getId());
        assertEquals(testValue2, capturedEvents.get(100).getNewValue());
        
    }
    
    /**
     * Test an exception thrown by an EntryProcessor leads to a correct rollback.
     */
    @Test
    public void testFailedInvocation() {
        
        CacheName cachename = new CacheName("test-cache1");
        NamedCache cache = transactionManager.getCache(cachename.getLogicalName());
        
        for (int i = 0; i < 100; i++) {
            cache.put(i, "value1");
        }
        
        transactionManager.getTransaction().commit();

        NamedCache vcache = CacheFactory.getCache(cachename.getVersionCacheName());

        assertEquals(100, vcache.size());
        
        boolean caught = false;
        try {
            cache.invokeAll(AlwaysFilter.INSTANCE, new ExceptionThrowingProcessor("value2"));
        } catch (RuntimeException ex) {
            caught = true;
        }
        
        assertTrue(caught);
        
        boolean rollbackOnly = false;
        
        try {
            transactionManager.getTransaction().commit();
        } catch (TransactionException ex) {
            rollbackOnly = true;
        }
        
        assertTrue(rollbackOnly);
        
        int vsize = vcache.size();
        
        assertTrue(vsize > 100 && vsize < 200);
        
        transactionManager.getTransaction().rollback();
        
        assertEquals(100, vcache.size());

    }

}
