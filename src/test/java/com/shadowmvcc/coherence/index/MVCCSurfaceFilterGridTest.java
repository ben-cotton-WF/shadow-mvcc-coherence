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

package com.shadowmvcc.coherence.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.littlegrid.support.SystemUtils;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
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

/**
 * Test the MVCCSurfaceFilter with a littlegrid.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCSurfaceFilterGridTest extends AbstractLittlegridTest {

    private static final String TESTCACHENAME = "testcache";
    private NamedCache testCache;

    /**
     * initialise cache.
     */
    @Before
    public void setUp() {
        System.out.println("***MVCCSurfaceFilterGridTest testUseIndex");
        System.out.println(SystemUtils.getSystemPropertiesWithPrefix("tangosol"));
        testCache = CacheFactory.getCache(TESTCACHENAME);
        testCache.addIndex(new MVCCExtractor(), false, null);
        testCache.addIndex(new PofExtractor(null,
                new SimplePofPath(VersionedKey.POF_LOGICALKEY), AbstractExtractor.KEY), false, null);
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
//        Set result = testCache.entrySet(new EqualsFilter(new PofExtractor(null,
//              TransactionalValue.POF_VALUE), "medium version"));
//
//        Assert.assertEquals(1, result.size());
//    }

    /**
     * Get an entrySet to see that we get the right versions back.
     */
    @Test
    public void testUseIndex() {

        System.out.println("***MVCCSurfaceFilterGridTest testUseIndex");
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

    /**
     * Does the SurfaceFilter work correctly when Anded with another filter?
     */
    @Test
    public void testAndFilter() {
        System.out.println("***MVCCSurfaceFilterGridTest testAndFilter");
        Map<VersionedKey<Integer>, String> expected = new HashMap<VersionedKey<Integer>, String>();

        putTestValue(expected, 100, BASETIME, "oldest version");

        TransactionId tid = new TransactionId(BASETIME + 999, 0, 0);

        Filter filter = new AndFilter(
                new MVCCSurfaceFilter<Integer>(tid), 
                new EqualsFilter(
                        new PofExtractor(null,
                                new SimplePofPath(VersionedKey.POF_LOGICALKEY), AbstractExtractor.KEY), 100)
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

    /**
     * Surface filter with a filter nested in it.
     */
    @Test
    public void testNestedFilter() {
        System.out.println("***MVCCSurfaceFilterGridTest testNestedFilter");
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

    /**
     * Surface filter with a key.
     */
    @Test
    public void testFilterWithSpecifiedKey() {
        System.out.println("***MVCCSurfaceFilterGridTest testFilterWithSpecifiedKey");
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

    /**
     * Test when restricted to partition by key association.
     */
    @Test
    public void testKeyAssociationFilter() {
        System.out.println("***MVCCSurfaceFilterGridTest testKeyAssociationFilter");
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

    /**
     * Put a test value in the version cache.
     * @param cache cache
     * @param key key
     * @param timestamp transaction id
     * @param value value
     */
    @SuppressWarnings("unchecked")
    private void putTestValue(@SuppressWarnings("rawtypes") final Map cache, final int key,
            final long timestamp, final String value) {
        VersionedKey<Integer> vkey = new VersionedKey<Integer>(key, new TransactionId(timestamp, 0, 0));

        cache.put(vkey, value);
    }

}
