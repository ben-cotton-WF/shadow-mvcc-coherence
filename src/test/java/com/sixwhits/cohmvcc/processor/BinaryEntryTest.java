package com.sixwhits.cohmvcc.processor;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.io.pof.PofContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.ExternalizableHelper;

public class BinaryEntryTest {
	
	private ClusterMemberGroup cmg;
	private static final String TESTCACHENAME = "testcache";
	private static final long BASETIME = 40L*365L*24L*60L*60L*1000L;
	private NamedCache testCache;
	private PofContext pofContext = new ConfigurablePofContext("mvcc-pof-config-test.xml");

	@Before
	public void setUp() throws Exception {
		System.setProperty("tangosol.pof.enabled", "true");
		DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
		cmg = builder.setStorageEnabledCount(1).build();

		System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
		testCache = CacheFactory.getCache(TESTCACHENAME);
		testCache.addIndex(new MVCCExtractor(), false, null);
	}
	
	@Test
	public void testEp() {
		putTestValue(testCache, 100, BASETIME, "a test value");

		VersionedKey<Integer> vkey = new VersionedKey<Integer>(100, new TransactionId(BASETIME, 0, 0));
		
		testCache.invoke(vkey, new DummyBinaryProcessor());
		
	}

	@After
	public void tearDown() throws Exception {
		cmg.shutdownAll();
	}

	
	@SuppressWarnings("unchecked")
	private void putTestValue(@SuppressWarnings("rawtypes") Map cache, int key, long timestamp, String value) {
		VersionedKey<Integer> vkey = new VersionedKey<Integer>(key, new TransactionId(timestamp, 0, 0));
		TransactionalValue tvalue = new TransactionalValue(true, false, ExternalizableHelper.toBinary(value, pofContext));
		cache.put(vkey, tvalue);
	}
	
}
