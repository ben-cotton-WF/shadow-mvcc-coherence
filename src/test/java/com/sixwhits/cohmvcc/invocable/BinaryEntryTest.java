package com.sixwhits.cohmvcc.invocable;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.testsupport.AbstractLittlegridTest;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Simple test of BinaryEntry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class BinaryEntryTest extends AbstractLittlegridTest {

    private static final String TESTCACHENAME = "testcache";
    private NamedCache testCache;

    /**
     * create cluster and initialise cache.
     */
    @Before
    public void setUp() {
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
