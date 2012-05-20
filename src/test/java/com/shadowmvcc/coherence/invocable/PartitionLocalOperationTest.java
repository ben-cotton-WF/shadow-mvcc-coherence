package com.shadowmvcc.coherence.invocable;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.shadowmvcc.coherence.transaction.SessionTransactionManager;
import com.shadowmvcc.coherence.transaction.SystemTimestampSource;
import com.shadowmvcc.coherence.transaction.TransactionManager;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Test use of partition local transactions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class PartitionLocalOperationTest extends AbstractLittlegridTest {

    private static final CacheName MVCCCACHE1NAME = new CacheName("mvcccache1");
    private NamedCache mvccCache1;
    private static final CacheName MVCCCACHE2NAME = new CacheName("mvcccache2");
    private NamedCache mvccCache2;
    private static final String SIMPLECACHENAME = "simplecache";
    private TransactionManager transactionManager;
    private static final String TESTKEY = "ABC";

    /**
     * create cluster and initialise cache.
     */
    @Before
    public void setUp() {
        transactionManager = new SessionTransactionManager(new SystemTimestampSource());
        mvccCache1 = transactionManager.getCache(MVCCCACHE1NAME.getLogicalName());
        mvccCache1.addIndex(new MVCCExtractor(), false, null);
        mvccCache2 = transactionManager.getCache(MVCCCACHE2NAME.getLogicalName());
        mvccCache2.addIndex(new MVCCExtractor(), false, null);
    }
    
    /**
     * Test a PLT between two MVCC caches.
     */
    @Test
    public void testPLTMVCC() {
        EntryProcessor ep = new PartitionLocalTransactionProcessor(MVCCCACHE2NAME.getLogicalName(), true);
        
        mvccCache1.invoke(TESTKEY, ep);
        
        transactionManager.getTransaction().commit();
        
        assertEquals(1, mvccCache1.get(TESTKEY));
        assertEquals(-1, mvccCache2.get(TESTKEY));
    }
    
    /**
     * Test a PLT between an MVCC cache and a simple cache.
     */
    @Test
    public void testPLTSimple() {
        EntryProcessor ep = new PartitionLocalTransactionProcessor(SIMPLECACHENAME, false);
        NamedCache simpleCache = CacheFactory.getCache(SIMPLECACHENAME);
        
        mvccCache1.invoke(TESTKEY, ep);
        
        transactionManager.getTransaction().commit();
        
        assertEquals(1, mvccCache1.get(TESTKEY));
        assertEquals(-1, simpleCache.get(TESTKEY));
    }
}
