package com.sixwhits.cohmvcc.invocable;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Simple test of BinaryEntry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class BinaryEntryTest {

    private ClusterMemberGroup cmg;
    private static final String TESTCACHENAME = "testcache";
    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;
    private NamedCache testCache;

    /**
     * initialise system properties.
     */
    @BeforeClass
    public static void setSystemProperties() {
        System.setProperty("tangosol.pof.enabled", "true");
        System.setProperty("pof-config-file", "mvcc-pof-config-test.xml");
    }


    /**
     * create cluster and initialise cache.
     */
    @Before
    public void setUp() {
        DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
        cmg = builder.setStorageEnabledCount(1).buildAndConfigureForStorageDisabledClient();

        testCache = CacheFactory.getCache(TESTCACHENAME);
        testCache.addIndex(new MVCCExtractor(), false, null);
    }

    /**
     * Test invoke.
     */
    @Test
    public void testEp() {
        putTestValue(testCache, 100, BASETIME, "a test value");

        VersionedKey<Integer> vkey = new VersionedKey<Integer>(100, new TransactionId(BASETIME, 0, 0));

        testCache.invoke(vkey, new DummyBinaryProcessor());

    }

    /**
     * shutdown the cluster.
     */
    @After
    public void tearDown() {
        CacheFactory.shutdown();
        cmg.shutdownAll();
    }


    /**
     * Put a test value in the cache.
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
