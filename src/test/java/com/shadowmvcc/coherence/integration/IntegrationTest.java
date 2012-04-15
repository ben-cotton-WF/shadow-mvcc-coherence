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
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.extractor.IdentityExtractor;
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
        System.setProperty("sixwhits.cohmvcc.opentransactiontimeout", "1000");
        System.setProperty("sixwhits.cohmvcc.transactioncompletiontimeout", "1000");
        System.setProperty("sixwhits.cohmvcc.pollinterval", "100");
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

}
