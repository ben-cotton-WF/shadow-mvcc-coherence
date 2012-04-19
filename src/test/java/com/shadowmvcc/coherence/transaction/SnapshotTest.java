package com.shadowmvcc.coherence.transaction;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Test creating a snapshot.
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SnapshotTest extends AbstractLittlegridTest {

    private TransactionManager transactionManager;
    private static final CacheName TESTCACHENAME = new CacheName("testcache");
    private static final int COUNTI = 10;
    private static final int COUNTJ = 10;
    
    /**
     * initialise system properties.
     */
    @BeforeClass
    public static void setMonitorProperties() {
        System.setProperty("shadowmvcc.opentransactiontimeout", "1000");
        System.setProperty("shadowmvcc.transactioncompletiontimeout", "1000");
        System.setProperty("shadowmvcc.pollinterval", "100");
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
     * Populate a cache over a set of transactions, then reduce it to
     * two consecutive snapshots.
     */
    @Test
    public void testCreateSnapshots() {
        
        NamedCache testCache = transactionManager.getCache(TESTCACHENAME.getLogicalName());
        List<TransactionId> ids = new ArrayList<TransactionId>(COUNTI);
        
        for (int i = 0; i < COUNTI; i++) {
            for (int j = i; j < i + COUNTJ; j++) {
                testCache.put(j, "testValue");
            }
            Transaction transaction = transactionManager.getTransaction();
            ids.add(transaction.getTransactionId());
            transaction.commit();
        }
        
        transactionManager.createSnapshot(TESTCACHENAME, ids.get(4));
        
        transactionManager.createSnapshot(TESTCACHENAME, ids.get(9));
        
        Set<VersionedKey<Integer>> expected = new HashSet<VersionedKey<Integer>>();
        
        for (int i = 0; i < COUNTI + 5 - 1; i++) {
            expected.add(new VersionedKey<Integer>(i, ids.get(i <= 4 ? i : 4)));
        }
        
        for (int i = 5; i < COUNTI + COUNTJ - 1; i++) {
            expected.add(new VersionedKey<Integer>(i, ids.get(i <= 9 ? i : 9)));
        }
        
        NamedCache versionCache = CacheFactory.getCache(TESTCACHENAME.getVersionCacheName());
        
        @SuppressWarnings("unchecked")
        Set<VersionedKey<Integer>> result = versionCache.keySet();
        
        Assert.assertEquals(expected.size(), result.size());
        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));
        
    }

}
