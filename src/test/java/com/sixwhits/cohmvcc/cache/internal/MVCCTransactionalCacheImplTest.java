package com.sixwhits.cohmvcc.cache.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.repeatableRead;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.domain.SampleDomainObject;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.transaction.internal.EntryCommitProcessor;
import com.sixwhits.cohmvcc.transaction.internal.EntryRollbackProcessor;
import com.tangosol.io.pof.PortableException;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.aggregator.LongSum;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.extractor.PofUpdater;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.processor.ExtractorProcessor;
import com.tangosol.util.processor.UpdaterProcessor;

public class MVCCTransactionalCacheImplTest {
	
	private ClusterMemberGroup cmg;
	private static final String TESTCACHEMAME = "testcache";
	private static final long BASETIME = 40L*365L*24L*60L*60L*1000L;
	private MVCCTransactionalCacheImpl<Integer, SampleDomainObject> cache;
	
	@BeforeClass
	public static void setSystemProperties() {
		System.setProperty("pof-config-file", "mvcc-pof-config-test.xml");
		System.setProperty("tangosol.pof.enabled", "true");
	}

	@Before
	public void setUp() throws Exception {
		System.out.println("******setUp");
		DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
		cmg = builder.setStorageEnabledCount(2).build();

		System.out.println("******initialise cache");
		System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
		cache = new MVCCTransactionalCacheImpl<Integer, SampleDomainObject>(TESTCACHEMAME);
	}

	@Test
	public void testPutCommitRead() {
		
		System.out.println("******PutCommitRead");
		
		final TransactionId ts = new TransactionId(BASETIME, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		assertNull(cache.put(ts, repeatableRead, false, theKey, theValue));
		
		asynchCommit(ts, theKey);
		
		TransactionId ts2 = new TransactionId(BASETIME + 60000, 0, 0);
		assertEquals(theValue, cache.get(ts2, repeatableRead, theKey));

	}

	@Test
	public void testContainsKey() {
		
		System.out.println("******ContainsKey");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		Integer otherKey = 98;
		
		assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
		assertNull(cache.put(ts3, repeatableRead, true, otherKey, theValue));
		
		
		assertTrue(cache.containsKey(ts2, repeatableRead, theKey));
		assertFalse(cache.containsKey(ts2, repeatableRead, otherKey));
		assertFalse(cache.containsKey(ts2, repeatableRead, 97));
	}
	
	@Test
	public void testContainsValue() {
		
		System.out.println("******ContainsValue");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject otherValue = new SampleDomainObject(99, "ninety-nine");
		SampleDomainObject noValue = new SampleDomainObject(77, "seventy-seven");
		Integer otherKey = 98;
		
		assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
		assertNull(cache.put(ts3, repeatableRead, true, otherKey, otherValue));
		
		
		assertTrue(cache.containsValue(ts2, repeatableRead, theValue));
		assertFalse(cache.containsValue(ts2, repeatableRead, otherValue));
		assertFalse(cache.containsValue(ts2, repeatableRead, noValue));
	}
	
	@Test
	public void testPutRemoveRead() {
		
		System.out.println("******PutRemoveRead");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+60000, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+120000, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
		cache.remove(ts2, repeatableRead, true, theKey);
		
		assertNull(cache.get(ts3, readCommitted, theKey));

	}
	@Test
	public void testPutRemoveReadCommit() {
		
		System.out.println("******PutRemoveReadCommit");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+60000, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+120000, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
		cache.remove(ts2, repeatableRead, false, theKey);
		
		Semaphore flag = new Semaphore(0);
		asynchCommit(flag, ts2, theKey);
		
		assertNull(cache.get(ts3, readUncommitted, theKey));
		
		flag.release();
		
		assertNull(cache.get(ts3, readCommitted, theKey));

	}

	@Test
	public void testPutRemoveReadRollback() {
		
		System.out.println("******PutRemoveRead");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+60000, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+120000, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
		cache.remove(ts2, repeatableRead, false, theKey);
		
		asynchRollback(ts2, theKey);
		
		assertEquals(theValue, cache.get(ts3, readCommitted, theKey));

	}
	
	@Test
	public void testInsertRollbackRead() {
		
		System.out.println("******InsertRollbackRead");
		
		final TransactionId ts = new TransactionId(BASETIME, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		assertNull(cache.put(ts, repeatableRead, false, theKey, theValue));
		
		asynchRollback(ts, theKey);
		
		TransactionId ts2 = new TransactionId(BASETIME + 60000, 0, 0);
		assertNull(cache.get(ts2, repeatableRead, theKey));

	}

	@Test(expected=PortableException.class)
	public void testPutEarlierPut() {
		
		System.out.println("******PutEarlierPut");
		
		final TransactionId tslater = new TransactionId(BASETIME+60000, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		assertNull(cache.put(tslater, repeatableRead, true, theKey, theValue));
		
		TransactionId tsearlier = new TransactionId(BASETIME, 0, 0);
		SampleDomainObject earliervalue = new SampleDomainObject(77, "seventy-seven");
		cache.put(tsearlier, repeatableRead, true, theKey, earliervalue);
	}

	@Test
	public void testInsertEarlierPut() {
		
		System.out.println("******InsertEarlierPut");
		
		final TransactionId tslater = new TransactionId(BASETIME+60000, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		cache.insert(tslater, true, theKey, theValue);
		
		TransactionId tsearlier = new TransactionId(BASETIME, 0, 0);
		SampleDomainObject earliervalue = new SampleDomainObject(77, "seventy-seven");
		assertNull(cache.put(tsearlier, repeatableRead, true, theKey, earliervalue));
	}
	
	@Test
	public void testInvoke() {
		
		System.out.println("******Invoke");
		
		final TransactionId ts = new TransactionId(BASETIME, 0, 0);
		Integer theKey = 99;
		SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
		
		cache.insert(ts, true, theKey, theValue);

		EntryProcessor ep = new ExtractorProcessor(new PofExtractor(null, SampleDomainObject.POF_INTV)); 
		
		final TransactionId ts2 = new TransactionId(BASETIME+60000, 0, 0);
		assertEquals(88, cache.invoke(ts2, repeatableRead, true, theKey, ep));
		
	}

	@Test
	public void testSize() {
		
		System.out.println("******Size");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);
		final TransactionId ts4 = new TransactionId(BASETIME+3, 0, 0);
		final TransactionId ts5 = new TransactionId(BASETIME+4, 0, 0);
		final TransactionId ts6 = new TransactionId(BASETIME+5, 0, 0);
		final TransactionId ts7 = new TransactionId(BASETIME+6, 0, 0);

		SampleDomainObject val2 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val4 = new SampleDomainObject(88, "eighty-eight");

		for (int key = 0; key < 3; key++) {
			cache.insert(ts2, true, key, val2);
		}
		
		for (int key = 0; key < 5; key++) {
			cache.insert(ts4, true, key, val4);
		}
		
		assertEquals(0, cache.size(ts1, repeatableRead));
		assertEquals(3, cache.size(ts3, repeatableRead));
		assertEquals(5, cache.size(ts5, repeatableRead));

		cache.insert(ts4, false, 6, val4);
		
		assertEquals(6, cache.size(ts5, readUncommitted));
		
		asynchCommit(ts4, 6);

		assertEquals(6, cache.size(ts5, repeatableRead));

		cache.insert(ts4, false, 7, val4);
		
		assertEquals(7, cache.size(ts5, readUncommitted));
		
		asynchRollback(ts4, 7);

		assertEquals(6, cache.size(ts5, repeatableRead));
		
		cache.remove(ts6, repeatableRead, true, 0);

		assertEquals(5, cache.size(ts7, repeatableRead));
	}

	@Test
	public void testEntrySet() {
		System.out.println("******EntrySet");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts1, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);
		
		Set<Map.Entry<Integer,SampleDomainObject>> entrySet = cache.entrySet(ts2, repeatableRead, filter);
		
		Map<Integer,SampleDomainObject> expected = new HashMap<Integer,SampleDomainObject>(5);
		expected.put(1,val2);
		expected.put(3,val2);
		expected.put(5,val2);
		expected.put(7,val2);
		expected.put(9,val2);
		
		assertEquals(5, entrySet.size());
		assertTrue(entrySet.containsAll(expected.entrySet()));
		
	}
	
	@Test
	public void testEntrySetAll() {
		System.out.println("******EntrySetAll");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts3, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		Set<Map.Entry<Integer,SampleDomainObject>> entrySet = cache.entrySet(ts2, repeatableRead);
		
		Map<Integer,SampleDomainObject> expected = new HashMap<Integer,SampleDomainObject>(5);
		expected.put(1,val2);
		expected.put(3,val2);
		expected.put(5,val2);
		expected.put(7,val2);
		expected.put(9,val2);
		
		assertEquals(5, entrySet.size());
		assertTrue(entrySet.containsAll(expected.entrySet()));
		
	}

	@Test
	public void testEntrySetWithUncommitted() {
		System.out.println("******EntrySet");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts1, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		cache.insert(ts1, true, 10, val1);
		cache.insert(ts1, true, 11, val2);
		cache.insert(ts1, true, 12, val1);
		cache.insert(ts1, true, 13, val2);
		
		cache.insert(ts2, false, 10, val2);
		cache.insert(ts2, false, 11, val1);
		cache.insert(ts2, false, 12, val2);
		cache.insert(ts2, false, 13, val1);
		
		Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);
	
		asynchCommit(ts2, 10);
		asynchCommit(ts2, 11);
		asynchRollback(ts2, 12);
		asynchRollback(ts2, 13);
		Set<Map.Entry<Integer,SampleDomainObject>> entrySet = cache.entrySet(ts3, repeatableRead, filter);
		
		Map<Integer,SampleDomainObject> expected = new HashMap<Integer,SampleDomainObject>(5);
		expected.put(1,val2);
		expected.put(3,val2);
		expected.put(5,val2);
		expected.put(7,val2);
		expected.put(9,val2);
		expected.put(10,val2);
		expected.put(13,val2);
		
		assertEquals(expected.size(), entrySet.size());
		assertTrue(entrySet.containsAll(expected.entrySet()));
		
	}

	@Test
	public void testKeySet() {
		System.out.println("******KeySet");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts1, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);
		
		Set<Integer> keySet = cache.keySet(ts2, repeatableRead, filter);
		
		Set<Integer> expected = new HashSet<Integer>(5);
		expected.add(1);
		expected.add(3);
		expected.add(5);
		expected.add(7);
		expected.add(9);
		
		assertEquals(5, keySet.size());
		assertTrue(keySet.containsAll(expected));
		
	}

	@Test
	public void testKeySetAll() {
		System.out.println("******KeySetAll");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts3, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		Set<Integer> keySet = cache.keySet(ts2, repeatableRead);
		
		Set<Integer> expected = new HashSet<Integer>(5);
		expected.add(1);
		expected.add(3);
		expected.add(5);
		expected.add(7);
		expected.add(9);
		
		assertEquals(5, keySet.size());
		assertTrue(keySet.containsAll(expected));
		
	}
	
	@Test
	public void testInvokeAllFilter() {
		System.out.println("******InvokeAll(Filter)");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts1, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);
		EntryProcessor ep = new UpdaterProcessor(new PofUpdater(SampleDomainObject.POF_STRV), "seventy-eight"); 
		
		Set<Integer> keySet = cache.invokeAll(ts2, repeatableRead, true, filter, ep).keySet();
		
		Set<Integer> expected = new HashSet<Integer>(5);
		expected.add(1);
		expected.add(3);
		expected.add(5);
		expected.add(7);
		expected.add(9);
		
		assertEquals(5, keySet.size());
		assertTrue(keySet.containsAll(expected));
		
		SampleDomainObject expectedObject = new SampleDomainObject(77, "seventy-eight");
		
		for (Integer key : expected) {
			assertEquals(expectedObject, cache.get(ts2, repeatableRead, key));
		}
		
	}
	@Test
	public void testInvokeAllKeys() {
		System.out.println("******InvokeAll(Keys)");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 4; key++) {
			cache.insert(ts1, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		cache.insert(ts1, false, 8, val1);
		cache.insert(ts1, false, 9, val2);
		
		asynchCommit(ts1, 8);
		asynchCommit(ts1, 9);
		
		EntryProcessor ep = new UpdaterProcessor(new PofUpdater(SampleDomainObject.POF_STRV), "seventy-eight"); 
		
		Set<Integer> expected = new HashSet<Integer>(5);
		expected.add(1);
		expected.add(3);
		expected.add(5);
		expected.add(7);
		expected.add(9);
		
		Set<Integer> keySet = cache.invokeAll(ts2, repeatableRead, true, expected, ep).keySet();
		
		
		assertEquals(5, keySet.size());
		assertTrue(keySet.containsAll(expected));
		
		SampleDomainObject expectedObject = new SampleDomainObject(77, "seventy-eight");
		
		for (Integer key : expected) {
			assertEquals(expectedObject, cache.get(ts2, repeatableRead, key));
		}
		
	}

	@Test
	public void testGetAll() {
		System.out.println("******GetAll");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts1, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		Set<Integer> keys = new HashSet<Integer>(5);
		keys.add(1);
		keys.add(3);
		keys.add(5);
		keys.add(7);
		keys.add(11);
		
		Map<Integer,SampleDomainObject> results = cache.getAll(ts2, repeatableRead, keys);
		
		Map<Integer,SampleDomainObject> expected = new HashMap<Integer,SampleDomainObject>(4);
		expected.put(1,val2);
		expected.put(3,val2);
		expected.put(5,val2);
		expected.put(7,val2);
		
		assertEquals(4, results.size());
		assertTrue(results.entrySet().containsAll(expected.entrySet()));
		
	}
	
	@Test
	public void testPutAll() {
		
		System.out.println("******PutAll");
		
		final TransactionId ts = new TransactionId(BASETIME, 0, 0);
		Map<Integer, SampleDomainObject> valueMap = new HashMap<Integer, SampleDomainObject>();
		for (Integer theKey = 0; theKey < 10; theKey++) {
			valueMap.put(theKey, new SampleDomainObject(theKey, "eighty-eight"));
		}
		
		cache.putAll(ts, true, valueMap);
		
		TransactionId ts2 = new TransactionId(BASETIME + 60000, 0, 0);
		
		assertEquals(10, cache.size(ts2, readCommitted));
		
		assertTrue(cache.entrySet(ts2, readCommitted).containsAll(valueMap.entrySet()));
		

	}

	@Test
	public void testClear() {
		
		System.out.println("******Clear");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);
		final TransactionId ts4 = new TransactionId(BASETIME+3, 0, 0);
		final TransactionId ts5 = new TransactionId(BASETIME+4, 0, 0);
		SampleDomainObject firstTranche = new SampleDomainObject(1, "first tranche");
		SampleDomainObject secondTranche = new SampleDomainObject(2, "second tranche");
		Map<Integer, SampleDomainObject> expected = new HashMap<Integer, SampleDomainObject>();
		for (Integer theKey = 0; theKey < 10; theKey++) {
			cache.insert(ts1, true, theKey, firstTranche);
			cache.insert(ts4, true, theKey + 10, secondTranche);
			expected.put(theKey + 10, secondTranche);
		}
		
		cache.clear(ts2, true);
		
		assertEquals(0, cache.size(ts3, readCommitted));
		assertEquals(10, cache.size(ts5, readCommitted));
		
		assertTrue(cache.entrySet(ts5, readCommitted).containsAll(expected.entrySet()));
		
	}
	
	@Test
	public void testValues() {
		System.out.println("******Values");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts3, true, key * 2, val1);
			cache.insert(ts1, true, key * 2 + 1, val2);
		}
		
		Collection<SampleDomainObject> values = cache.values(ts2, repeatableRead);
		
		Map<Integer,SampleDomainObject> expected = new HashMap<Integer,SampleDomainObject>(5);
		expected.put(1,val2);
		expected.put(3,val2);
		expected.put(5,val2);
		expected.put(7,val2);
		expected.put(9,val2);
		
		assertEquals(5, values.size());
		for (SampleDomainObject value : values) {
			assertEquals(val2, value);
		}
		
	}

	@Test
	public void testAggregateFilter() {
		
		System.out.println("******AggregateFilter");
		
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);

		SampleDomainObject val2 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val4 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 3; key++) {
			cache.insert(ts2, true, key, val2);
		}
		
		for (int key = 0; key < 5; key++) {
			cache.insert(ts2, true, key + 10, val4);
		}
		
		assertEquals(3,
				cache.aggregate(ts3, repeatableRead, new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 88), new Count()));
	}
	
	@Test
	public void testAggregateKeys() {
		
		System.out.println("******AggregateKeys");
		
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);

		SampleDomainObject val2 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val4 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 3; key++) {
			cache.insert(ts2, true, key, val2);
		}
		
		for (int key = 0; key < 5; key++) {
			cache.insert(ts2, true, key + 10, val4);
		}
		
		Set<Integer> keys = new HashSet<Integer>();
		keys.add(2);
		keys.add(10);
		keys.add(11);
		keys.add(20);
		assertEquals(Long.valueOf(88 + 77 + 77),
				cache.aggregate(ts3, repeatableRead, keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));
		
		cache.insert(ts2, false, 20, val4);

		assertEquals(Long.valueOf(88 + 77 + 77 + 77),
				cache.aggregate(ts3, readUncommitted, keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));
		
		asynchCommit(ts2, 20);

		assertEquals(Long.valueOf(88 + 77 + 77 + 77),
				cache.aggregate(ts3, readCommitted, keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));

		assertEquals(Long.valueOf(88 + 77 + 77 + 77),
				cache.aggregate(ts3, repeatableRead, keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));
	}
	
	private void asynchCommit(final TransactionId ts, final Integer key) {
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
				NamedCache vcache = CacheFactory.getCache(cache.cacheName.getVersionCacheName());
				vcache.invoke(new VersionedKey<Integer>(key, ts), new EntryCommitProcessor());
			}
		}).start();
	}
	
	private void asynchCommit(final Semaphore flag, final TransactionId ts, final Integer key) {
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					flag.acquire();
				} catch (InterruptedException e) {
				}
				NamedCache vcache = CacheFactory.getCache(cache.cacheName.getVersionCacheName());
				vcache.invoke(new VersionedKey<Integer>(key, ts), new EntryCommitProcessor());
			}
		}).start();
	}

	private void asynchRollback(final TransactionId ts, final Integer key) {
		Thread rbThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
				NamedCache vcache = CacheFactory.getCache(cache.cacheName.getVersionCacheName());
				vcache.invoke(new VersionedKey<Integer>(key, ts), new EntryRollbackProcessor());
			}
		});
		
		rbThread.start();
	}
	
	@After
	public void tearDown() throws Exception {
		System.out.println("******tearDown");
		CacheFactory.shutdown();
		cmg.shutdownAll();
	}

	
}
