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

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.shadowmvcc.coherence.transaction.SystemTimestampSource;
import com.shadowmvcc.coherence.transaction.ThreadTransactionManager;
import com.shadowmvcc.coherence.transaction.TimestampSource;
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

        Thread.sleep(1500);
        
        Assert.assertEquals(0, vcache.size());

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

        Assert.assertEquals(MapEvent.ENTRY_DELETED, capturedEvents.get(1).getId());
        Assert.assertEquals(MapEvent.ENTRY_UPDATED, capturedEvents.get(2).getId());
        Assert.assertEquals(testValue2, capturedEvents.get(2).getNewValue());
        Assert.assertEquals(MapEvent.ENTRY_INSERTED, capturedEvents.get(100).getId());
        Assert.assertEquals(testValue2, capturedEvents.get(100).getNewValue());
        
    }

}
