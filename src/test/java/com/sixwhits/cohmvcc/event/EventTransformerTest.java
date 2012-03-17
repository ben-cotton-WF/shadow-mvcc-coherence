package com.sixwhits.cohmvcc.event;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.serializable;
import static com.tangosol.util.MapEvent.ENTRY_INSERTED;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.cache.internal.UnconditionalRemoveProcessor;
import com.sixwhits.cohmvcc.domain.EventValue;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.invocable.MVCCEntryProcessorWrapper;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.MapEventTransformerFilter;
import com.tangosol.util.processor.ConditionalPut;

/**
 * Test the EventTransformer.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class EventTransformerTest {

    private ClusterMemberGroup cmg;
    private static final CacheName CACHENAME = new CacheName("testcache");
    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;
    private NamedCache versionCache;
    private NamedCache keyCache;
    private final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<MapEvent>(100);

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
        System.setProperty("tangosol.pof.enabled", "true");
        DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
        cmg = builder.setStorageEnabledCount(1).build();

        System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
        versionCache = CacheFactory.getCache(CACHENAME.getVersionCacheName());
        versionCache.addIndex(new MVCCExtractor(), false, null);
        keyCache = CacheFactory.getCache(CACHENAME.getKeyCacheName());

        events.clear();

    }

    /**
     * Add a listener for the test.
     * 
     * @param isolationLevel isolation level
     */
    private void addListener(final IsolationLevel isolationLevel) {
        MapListener testMapListener = new MapListener() {
            @Override
            public void entryUpdated(final MapEvent mapevent) {
                events.add(mapevent);
            }
            @Override
            public void entryInserted(final MapEvent mapevent) {
                events.add(mapevent);
            }
            @Override
            public void entryDeleted(final MapEvent mapevent) {
                events.add(mapevent);
            }
        };
        TransactionId tsevent = new TransactionId(BASETIME - 1, 0, 0);

        events.clear();

        versionCache.addMapListener(testMapListener, 
                new MapEventTransformerFilter(AlwaysFilter.INSTANCE,
                        new MVCCEventTransformer<Integer, String>(isolationLevel, tsevent, CACHENAME)), false);

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
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformInsert() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);

        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, true, false);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformInsertUncommitted() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue, false);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);

        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, false, false);

    }
    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformIgnoreUncommitted() throws InterruptedException {

        addListener(readCommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue, false);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);

        assertNull(event);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformIgnoreBackdated() throws InterruptedException {

        addListener(readCommitted);

        TransactionId ts = new TransactionId(BASETIME - 2, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);

        assertNull(event);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformUpdate() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);

        String testValue = "a test value";
        String testValue2 = "updated test value";

        put(99, ts, testValue);
        put(99, ts2, testValue2);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, true, false);

        event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts2, testValue, testValue2, true, false);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformUpdateBackdated() throws InterruptedException {

        addListener(readCommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);

        String testValue = "a test value";
        String testValue2 = "updated test value";

        put(99, ts2, testValue2);
        put(99, ts, testValue);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts2, null, testValue2, true, false);

        event = events.poll(1, TimeUnit.SECONDS);
        assertNull(event);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformUpdateBackdatedUncommitted() throws InterruptedException {

        addListener(readCommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);
        TransactionId ts3 = new TransactionId(BASETIME + 2, 0, 0);

        String testValue = "a test value";
        String testValue3 = "updated test value";
        String testValue2 = "second test value";

        put(99, ts3, testValue3);
        put(99, ts2, testValue2, false);
        put(99, ts, testValue);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts3, null, testValue3, true, false);

        event = events.poll(1, TimeUnit.SECONDS);
        assertNull(event);

    }
    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testUpdateUncommittedReadUncomitted() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);
        TransactionId ts3 = new TransactionId(BASETIME + 2, 0, 0);

        String testValue = "a test value";
        String testValue2 = "updated test value";
        String testValue3 = "third test value";

        put(99, ts, testValue);
        put(99, ts2, testValue2, false);
        put(99, ts3, testValue3);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, true, false);

        event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts2, testValue, testValue2, false, false);

        event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts3, testValue2, testValue3, true, false);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testUpdateUncommittedReadCommitted() throws InterruptedException {

        addListener(readCommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);
        TransactionId ts3 = new TransactionId(BASETIME + 2, 0, 0);

        String testValue = "a test value";
        String testValue2 = "updated test value";
        String testValue3 = "third test value";

        put(99, ts, testValue);
        put(99, ts2, testValue2, false);
        put(99, ts3, testValue3);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, true, false);

//      No event for ts2 - suppressed by readCommitted listener

        event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts3, testValue, testValue3, true, false);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testTransformDelete() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);
        remove(99, ts2);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, true, false);

        event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts2, testValue, null, true, true);

    }

    /**
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testIgnoreVersionReap() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);
        versionCache.remove(new VersionedKey<Integer>(99, ts));

        MapEvent event = events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, true, false);

        event = events.poll(1, TimeUnit.SECONDS);
        assertNull(event);

    }

    /**
     * Put a test value in the cache.
     * @param key key
     * @param ts transaction id
     * @param value value
     */
    private void put(final Integer key, final TransactionId ts, final String value) {
        put(key, ts, value, true);
    }

    /**
     * Put a test value in the cache.
     * @param key key
     * @param ts transaction id
     * @param value value
     * @param autocommit auto commit if true
     */
    private void put(final Integer key, final TransactionId ts, final String value,
            final boolean autocommit) {
        EntryProcessor insertProcessor = new ConditionalPut(AlwaysFilter.INSTANCE, value);
        EntryProcessor wrapper = new MVCCEntryProcessorWrapper<String, Object>(
                ts, insertProcessor, readUncommitted, autocommit, CACHENAME);
        keyCache.invoke(99, wrapper);
    }

    /**
     * Remove an entry from the cache.
     * @param key key
     * @param ts transaction id
     */
    private void remove(final Integer key, final TransactionId ts) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<String, Object>(
                ts, new UnconditionalRemoveProcessor(), serializable, true, CACHENAME);
        keyCache.invoke(99, ep);
    }

    /**
     * Check an event has expected contents.
     * @param event actual event to check
     * @param expectedType expected type
     * @param ts expected transaction id
     * @param oldExpected old value
     * @param newExpected new value
     * @param isCommitted committed
     * @param isDeleted deleted
     */
    private void checkEvent(final MapEvent event, final int expectedType, final TransactionId ts, 
            final String oldExpected, final String newExpected, final boolean isCommitted, final boolean isDeleted) {
        @SuppressWarnings("unchecked")
        EventValue<String> ev = (EventValue<String>) event.getNewValue();
        assertNotNull(event);
        Assert.assertEquals(new VersionedKey<Integer>(99, ts), event.getKey());
        assertEquals(newExpected, ev.getValue());
        assertEquals(oldExpected, event.getOldValue());
        assertEquals(expectedType, event.getId());
        assertEquals(isCommitted, ev.isCommitted());
        assertEquals(isDeleted, ev.isDeleted());

    }

}
