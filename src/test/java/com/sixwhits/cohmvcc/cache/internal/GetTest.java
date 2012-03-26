package com.sixwhits.cohmvcc.cache.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.SampleDomainObject;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.invocable.MVCCReadOnlyEntryProcessorWrapper;
import com.sixwhits.cohmvcc.testsupport.AbstractLittlegridTest;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.extractor.IdentityExtractor;
import com.tangosol.util.processor.ExtractorProcessor;

/**
 * Test cache get.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class GetTest extends AbstractLittlegridTest {

    private static final String TESTCACHEMAME = "testcache";

    /**
     * create cluster and initialise cache.
     */
    @Before
    public void setUp() {
        System.out.println("******initialise cache");
        new MVCCTransactionalCacheImpl<Integer, SampleDomainObject>(TESTCACHEMAME, "InvocationService");
    }

    /**
     * test get.
     */
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
                new MVCCReadOnlyEntryProcessorWrapper<Integer, String>(
                        ts, new ExtractorProcessor(new IdentityExtractor()), readCommitted, cacheName));

        assertEquals(theValue, result.getResult());

    }

}
