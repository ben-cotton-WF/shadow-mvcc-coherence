package com.shadowmvcc.coherence.transaction;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.config.ClusterTimeProviderFactory;
import com.shadowmvcc.coherence.config.OffsettableClusterTimeProvider;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.shadowmvcc.coherence.transaction.internal.SnapshotManagerImpl;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.ClassHelper;

/**
 * Test creating a snapshot.
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SnapshotTest extends AbstractLittlegridTest {

    private TransactionManager transactionManager;
    private SnapshotManager snapshotManager;
    private OffsettableClusterTimeProvider timeProvider;
    private static final CacheName TESTCACHENAME = new CacheName("testcache");
    private static final int COUNTI = 10;
    private static final int COUNTJ = 10;
    
    /**
     * initialise system properties.
     */
    @BeforeClass
    public static void setProperties() {
        System.setProperty("shadowmvcc.opentransactiontimeout", "1000");
        System.setProperty("shadowmvcc.transactioncompletiontimeout", "1000");
        System.setProperty("shadowmvcc.transactionpollinterval", "100");
        System.setProperty("shadowmvcc.minsnapshotage", "400000");
        
        System.setProperty("shadowmvcc.timeproviderclass", OffsettableClusterTimeProvider.class.getName());
    }
    
    /**
     * Set up the tm.
     */
    @Before
    public void initialiseTransactionManager() {
        
        TimestampSource timestampSource = new SystemTimestampSource();
        
        transactionManager = new ThreadTransactionManager(
                timestampSource, false, false, readCommitted);
        
        snapshotManager = new SnapshotManagerImpl();
        
        timeProvider = (OffsettableClusterTimeProvider) ClusterTimeProviderFactory.getInstance();
        
    }
    
    /**
     * Populate a cache over a set of transactions, then reduce it to
     * two consecutive snapshots.
     * @throws InterruptedException never
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     * @throws NoSuchMethodException 
     * @throws ClassNotFoundException 
     */
    @Test
    public void testCreateSnapshots() throws InterruptedException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        
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
        
        setTimeOffset(500000);
        
        snapshotManager.createSnapshot(TESTCACHENAME, ids.get(4).getTimeStampMillis());
        
        snapshotManager.createSnapshot(TESTCACHENAME, ids.get(9).getTimeStampMillis());
        
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

    /**
     * Populate a cache over a set of transactions, then reduce it to
     * two consecutive snapshots, finally coalesce from big bang to the second
     * snapshot to remove the intermediate.
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     * @throws NoSuchMethodException 
     * @throws ClassNotFoundException 
     */
    @Test
    public void testCoalesceSnapshots() throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        
        setTimeOffset(0);
        
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
        
        setTimeOffset(500000);

        snapshotManager.createSnapshot(TESTCACHENAME, ids.get(4).getTimeStampMillis());
        
        snapshotManager.createSnapshot(TESTCACHENAME, ids.get(9).getTimeStampMillis());
        
        snapshotManager.coalesceSnapshots(TESTCACHENAME, ids.get(9).getTimeStampMillis());
        
        Set<VersionedKey<Integer>> expected = new HashSet<VersionedKey<Integer>>();
        
        for (int i = 0; i < 5; i++) {
            expected.add(new VersionedKey<Integer>(i, ids.get(i)));
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
    
    /**
     * Set a time offset in all the members.
     * @param offset the offset
     * @throws NoSuchMethodException maybe
     * @throws IllegalAccessException maybe
     * @throws InvocationTargetException maybe
     * @throws ClassNotFoundException maybe
     */
    private void setTimeOffset(final long offset) throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        timeProvider.setOffset(offset);
        
        for (int member : this.getClusterMemberGroup().getStartedMemberIds()) {
            final ClassLoader containingClassLoader = 
                    getClusterMemberGroup().getClusterMember(member).getActualContainingClassLoader();
            Class<?> memberFactoryClass = containingClassLoader.loadClass(ClusterTimeProviderFactory.class.getName());
            Object memberTimeProvider = 
                    ClassHelper.invokeStatic(
                    memberFactoryClass, "getInstance", new Object[]{});
            ClassHelper.invoke(memberTimeProvider, "setOffset", new Object[] { Long.valueOf(offset) });
        }
        
    }

}
