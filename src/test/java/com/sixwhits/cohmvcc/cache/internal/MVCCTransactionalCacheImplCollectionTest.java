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
import org.junit.Ignore;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.domain.SampleDomainObject;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.transaction.internal.EntryCommitProcessor;
import com.sixwhits.cohmvcc.transaction.internal.EntryRollbackProcessor;
import com.tangosol.coherence.reporter.locator.SumLocator;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.aggregator.LongSum;
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
	
}
