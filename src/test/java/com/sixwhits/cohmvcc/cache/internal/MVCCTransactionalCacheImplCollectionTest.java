package com.sixwhits.cohmvcc.cache.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.SampleDomainObject;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.transaction.internal.EntryCommitProcessor;
import com.sixwhits.cohmvcc.transaction.internal.EntryRollbackProcessor;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.filter.EqualsFilter;

public class MVCCTransactionalCacheImplCollectionTest {
	
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
		System.setProperty("tangosol.pof.enabled", "true");
		DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
		cmg = builder.setStorageEnabledCount(1).build();

		System.out.println("******initialise cache");
		System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
		cache = new MVCCTransactionalCacheImpl<Integer, SampleDomainObject>(TESTCACHEMAME);
	}

	@Test
	public void testSize() {
		
		System.out.println("******Size");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);
		final TransactionId ts3 = new TransactionId(BASETIME+2, 0, 0);
		final TransactionId ts4 = new TransactionId(BASETIME+3, 0, 0);
		final TransactionId ts5 = new TransactionId(BASETIME+4, 0, 0);

		SampleDomainObject val2 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val4 = new SampleDomainObject(88, "eighty-eight");

		for (int key = 0; key < 3; key++) {
			cache.insert(ts2, IsolationLevel.repeatableRead, true, key, val2);
		}
		
		for (int key = 0; key < 5; key++) {
			cache.insert(ts4, IsolationLevel.repeatableRead, true, key, val4);
		}
		
		Assert.assertEquals(0, cache.size(ts1, IsolationLevel.repeatableRead));
		Assert.assertEquals(3, cache.size(ts3, IsolationLevel.repeatableRead));
		Assert.assertEquals(5, cache.size(ts5, IsolationLevel.repeatableRead));

		cache.insert(ts4, IsolationLevel.repeatableRead, false, 6, val4);
		
		Assert.assertEquals(6, cache.size(ts5, IsolationLevel.readUncommitted));
		
		asynchCommit(ts4, 6);

		Assert.assertEquals(6, cache.size(ts5, IsolationLevel.repeatableRead));

		cache.insert(ts4, IsolationLevel.repeatableRead, false, 7, val4);
		
		Assert.assertEquals(7, cache.size(ts5, IsolationLevel.readUncommitted));
		
		asynchRollback(ts4, 7);

		Assert.assertEquals(6, cache.size(ts5, IsolationLevel.repeatableRead));
	}

	@Test
	public void testEntrySet() {
		System.out.println("******EntrySet");
		
		final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
		final TransactionId ts2 = new TransactionId(BASETIME+1, 0, 0);

		SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
		SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

		for (int key = 0; key < 5; key++) {
			cache.insert(ts1, IsolationLevel.repeatableRead, true, key * 2, val1);
			cache.insert(ts1, IsolationLevel.repeatableRead, true, key * 2 + 1, val2);
		}
		
		Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);
		
		Set<Map.Entry<Integer,SampleDomainObject>> entrySet = cache.entrySet(ts2, IsolationLevel.repeatableRead, filter);
		
		Map<Integer,SampleDomainObject> expected = new HashMap<Integer,SampleDomainObject>(5);
		expected.put(1,val2);
		expected.put(3,val2);
		expected.put(5,val2);
		expected.put(7,val2);
		expected.put(9,val2);
		
		Assert.assertEquals(5, entrySet.size());
		Assert.assertTrue(entrySet.containsAll(expected.entrySet()));
		
	}
	
	private void asynchCommit(final TransactionId ts, final Integer key) {
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
				NamedCache vcache = CacheFactory.getCache(cache.vcacheName);
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
				NamedCache vcache = CacheFactory.getCache(cache.vcacheName);
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
				NamedCache vcache = CacheFactory.getCache(cache.vcacheName);
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
