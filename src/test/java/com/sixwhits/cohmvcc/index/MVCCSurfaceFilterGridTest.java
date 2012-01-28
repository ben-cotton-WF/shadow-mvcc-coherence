package com.sixwhits.cohmvcc.index;

import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.oracle.common.collections.AbstractKeyBasedMap.EntrySet;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionStatus;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.filter.EqualsFilter;

public class MVCCSurfaceFilterGridTest {
	
	private ClusterMemberGroup cmg;
	private static final String TESTCACHENAME = "testcache";
	private static final long BASETIME = 40L*365L*24L*60L*60L*1000L;

	@Before
	public void setUp() throws Exception {
		System.setProperty("tangosol.pof.enabled", "true");
		DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
		cmg = builder.setStorageEnabledCount(1).build();
	}

	@After
	public void tearDown() throws Exception {
		cmg.shutdownAll();
	}

	/**
	 * Trivial test to allow tracing of the workings of a normal index.
	 * Disable for normal testing
	 */
	@Ignore
	@Test
	public void testEqualsIndex() {
		System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
		NamedCache testCache = CacheFactory.getCache(TESTCACHENAME);
		testCache.addIndex(new PofExtractor(null, TransactionalValue.POF_VALUE), false, null);
		putTestValue(testCache, 100, BASETIME, "oldest version");
		putTestValue(testCache, 100, BASETIME +1000, "medium version");
		putTestValue(testCache, 100, BASETIME +2000, "newest version");
		Set result = testCache.entrySet(new EqualsFilter(new PofExtractor(null, TransactionalValue.POF_VALUE), "medium version"));
		
		Assert.assertEquals(1, result.size());
	}

	@Test
	public void testUseIndex() {
		System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
		NamedCache testCache = CacheFactory.getCache(TESTCACHENAME);
		testCache.addIndex(new MVCCExtractor(), false, null);
		putTestValue(testCache, 100, BASETIME, "oldest version");
		putTestValue(testCache, 100, BASETIME +1000, "medium version");
		putTestValue(testCache, 100, BASETIME +2000, "newest version");
		
		Set<Map.Entry> result = queryForTime(testCache, BASETIME+999);
		
		Assert.assertEquals(1, result.size());
		Map.Entry entry = result.iterator().next();
		VersionedKey<Integer> k = (VersionedKey<Integer>) entry.getKey();
		Assert.assertEquals(Integer.valueOf(100), k.getNativeKey());
		Assert.assertEquals(BASETIME, k.getTxTimeStamp().getTimeStampMillis());
		
		
	}
	
	private void putTestValue(NamedCache cache, int key, long timestamp, String value) {
		VersionedKey<Integer> vkey = new VersionedKey<Integer>(key, new TransactionId(timestamp, 0, 0));
		TransactionalValue<String> tvalue = new TransactionalValue<String>(TransactionStatus.committed, value);
		cache.put(vkey, tvalue);
	}
	
	private Set queryForTime(NamedCache cache, long timestamp) {
		TransactionId tid = new TransactionId(timestamp, 0, 0);
		return cache.entrySet(new MVCCSurfaceFilter(tid));
		
	}

}
