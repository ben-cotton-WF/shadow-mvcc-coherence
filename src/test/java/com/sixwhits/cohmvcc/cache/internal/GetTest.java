package com.sixwhits.cohmvcc.cache.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.SampleDomainObject;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.invocable.MVCCReadOnlyEntryProcessorWrapper;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.processor.ExtractorProcessor;

public class GetTest {

    private ClusterMemberGroup cmg;
    private static final String TESTCACHEMAME = "testcache";
    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;

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
        new MVCCTransactionalCacheImpl<Integer, SampleDomainObject>(TESTCACHEMAME, "InvocationService");
    }

    @Test
    public void testGet() {

        final TransactionId ts = new TransactionId(BASETIME, 0, 0);
        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        CacheName cacheName = new CacheName(TESTCACHEMAME);
        NamedCache keyCache = CacheFactory.getCache(cacheName.getKeyCacheName());
        NamedCache versionCache = CacheFactory.getCache(cacheName.getVersionCacheName());

        versionCache.put(new VersionedKey<Integer>(99, ts), theValue);
        @SuppressWarnings("unchecked")
        ProcessorResult<Integer, String> result = (ProcessorResult<Integer, String>) keyCache.invoke(theKey, 
                new MVCCReadOnlyEntryProcessorWrapper<Integer, String>(ts, new ExtractorProcessor(new IdentityExtractor()), readCommitted, cacheName));

        assertEquals(theValue, result.getResult());

    }

    @After
    public void tearDown() throws Exception {
        System.out.println("******tearDown");
        CacheFactory.shutdown();
        cmg.shutdownAll();
    }


}
