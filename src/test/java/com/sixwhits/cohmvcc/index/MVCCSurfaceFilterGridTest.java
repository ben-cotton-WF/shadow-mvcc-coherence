package com.sixwhits.cohmvcc.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.aggregator.QueryRecorder;
import com.tangosol.util.aggregator.QueryRecorder.RecordType;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.filter.AndFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.KeyAssociatedFilter;

public class MVCCSurfaceFilterGridTest {

    private ClusterMemberGroup cmg;
    private static final String TESTCACHENAME = "testcache";
    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;
    private NamedCache testCache;

    @Before
    public void setUp() throws Exception {
        System.setProperty("tangosol.pof.enabled", "true");
        DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
        cmg = builder.setStorageEnabledCount(1).build();

        System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
        testCache = CacheFactory.getCache(TESTCACHENAME);
        testCache.addIndex(new MVCCExtractor(), false, null);
        testCache.addIndex(new PofExtractor(null, new SimplePofPath(VersionedKey.POF_KEY), AbstractExtractor.KEY), false, null);
        putTestValue(testCache, 100, BASETIME, "oldest version");
        putTestValue(testCache, 100, BASETIME + 1000, "medium version");
        putTestValue(testCache, 100, BASETIME + 2000, "newest version");
        putTestValue(testCache, 101, BASETIME + 1000, "oldest version");
        putTestValue(testCache, 101, BASETIME + 2000, "medium version");
        putTestValue(testCache, 101, BASETIME + 3000, "newest version");
        putTestValue(testCache, 102, BASETIME, "oldest version");
        putTestValue(testCache, 102, BASETIME + 100, "medium version");
        putTestValue(testCache, 102, BASETIME + 200, "newest version");
    }

    @After
    public void tearDown() throws Exception {
        CacheFactory.shutdown();
        cmg.shutdownAll();
    }

    /**
     * Trivial test to allow tracing of the workings of a normal index.
     * Disable for normal testing
     */
//    @Ignore
//    @Test
//    public void testEqualsIndex() {
//        System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
//        NamedCache testCache = CacheFactory.getCache(TESTCACHENAME);
//        testCache.addIndex(new PofExtractor(null, TransactionalValue.POF_VALUE), false, null);
//        putTestValue(testCache, 100, BASETIME, "oldest version");
//        putTestValue(testCache, 100, BASETIME +1000, "medium version");
//        putTestValue(testCache, 100, BASETIME +2000, "newest version");
//        @SuppressWarnings("rawtypes")
//        Set result = testCache.entrySet(new EqualsFilter(new PofExtractor(null, TransactionalValue.POF_VALUE), "medium version"));
//
//        Assert.assertEquals(1, result.size());
//    }

    @Test
    public void testUseIndex() {

        Map<VersionedKey<Integer>, String> expected = new HashMap<VersionedKey<Integer>, String>();
        putTestValue(expected, 100, BASETIME, "oldest version");
        putTestValue(expected, 102, BASETIME + 200, "newest version");

        TransactionId tid = new TransactionId(BASETIME + 999, 0, 0);
        @SuppressWarnings("unchecked")
        Set<Map.Entry<VersionedKey<Integer>, String>> result = testCache.entrySet(new MVCCSurfaceFilter<Integer>(tid));

        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsAll(expected.entrySet()));
        Assert.assertTrue(expected.entrySet().containsAll(result));

    }

    @Test
    public void testAndFilter() {
        Map<VersionedKey<Integer>, String> expected = new HashMap<VersionedKey<Integer>, String>();

        putTestValue(expected, 100, BASETIME, "oldest version");

        TransactionId tid = new TransactionId(BASETIME + 999, 0, 0);

        Filter filter = new AndFilter(
                new MVCCSurfaceFilter<Integer>(tid), 
                new EqualsFilter(
                        new PofExtractor(null, new SimplePofPath(VersionedKey.POF_KEY), AbstractExtractor.KEY), 100)
                );

        Object resultsExplain = testCache.aggregate(filter, new QueryRecorder(RecordType.EXPLAIN));
        System.out.println(resultsExplain);
        Object resultsTrace = testCache.aggregate(filter, new QueryRecorder(RecordType.TRACE));
        System.out.println(resultsTrace);
        @SuppressWarnings("unchecked")
        Set<Map.Entry<VersionedKey<Integer>, String>> result = testCache.entrySet(filter);

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsAll(expected.entrySet()));
        Assert.assertTrue(expected.entrySet().containsAll(result));

    }

    @Test
    public void testNestedFilter() {
        Map<VersionedKey<Integer>, String> expected = new HashMap<VersionedKey<Integer>, String>();

        putTestValue(expected, 100, BASETIME, "oldest version");

        TransactionId tid = new TransactionId(BASETIME + 999, 0, 0);

        Filter filter = new MVCCSurfaceFilter<Integer>(tid, 
                new EqualsFilter(
                        IdentityExtractor.INSTANCE, "oldest version"));

        Object resultsExplain = testCache.aggregate(filter, new QueryRecorder(RecordType.EXPLAIN));
        System.out.println(resultsExplain);
        Object resultsTrace = testCache.aggregate(filter, new QueryRecorder(RecordType.TRACE));
        System.out.println(resultsTrace);
        @SuppressWarnings("unchecked")
        Set<Map.Entry<VersionedKey<Integer>, String>> result = testCache.entrySet(filter);

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsAll(expected.entrySet()));
        Assert.assertTrue(expected.entrySet().containsAll(result));

    }

    @Test
    public void testFilterWithSpecifiedKey() {
        Map<VersionedKey<Integer>, String> expected = new HashMap<VersionedKey<Integer>, String>();

        putTestValue(expected, 100, BASETIME, "oldest version");

        TransactionId tid = new TransactionId(BASETIME + 999, 0, 0);
        @SuppressWarnings("unchecked")
        Set<Map.Entry<VersionedKey<Integer>, String>> result = testCache.entrySet(
                        new MVCCSurfaceFilter<Integer>(tid, Collections.singleton(100)));

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsAll(expected.entrySet()));
        Assert.assertTrue(expected.entrySet().containsAll(result));

    }

    @Test
    public void testKeyAssociationFilter() {
        Map<VersionedKey<Integer>, String> expected = new HashMap<VersionedKey<Integer>, String>();

        putTestValue(expected, 100, BASETIME, "oldest version");

        TransactionId tid = new TransactionId(BASETIME + 999, 0, 0);

        VersionedKey<Integer> sampleKey = new VersionedKey<Integer>(100, tid);
        Filter filter = new MVCCSurfaceFilter<Integer>(tid, Collections.singleton(100));
        Filter keyFilter = new KeyAssociatedFilter(filter, sampleKey.getAssociatedKey());
        @SuppressWarnings("unchecked")
        Set<Map.Entry<VersionedKey<Integer>, String>> result = testCache.entrySet(keyFilter);

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.containsAll(expected.entrySet()));
        Assert.assertTrue(expected.entrySet().containsAll(result));

    }

    @SuppressWarnings("unchecked")
    private void putTestValue(@SuppressWarnings("rawtypes") Map cache, int key, long timestamp, String value) {
        VersionedKey<Integer> vkey = new VersionedKey<Integer>(key, new TransactionId(timestamp, 0, 0));

        cache.put(vkey, value);
    }

}
