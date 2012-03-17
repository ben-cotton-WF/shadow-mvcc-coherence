package com.sixwhits.cohmvcc.event;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.readUncommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.serializable;
import static com.tangosol.util.MapEvent.ENTRY_DELETED;
import static com.tangosol.util.MapEvent.ENTRY_INSERTED;
import static com.tangosol.util.MapEvent.ENTRY_UPDATED;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.coherence.testsupport.ClusterMemberGroup;
import org.littlegrid.coherence.testsupport.SystemPropertyConst;
import org.littlegrid.coherence.testsupport.impl.DefaultClusterMemberGroupBuilder;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.cache.internal.UnconditionalRemoveProcessor;
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

public class MVCCMapListenerTest {

    private ClusterMemberGroup cmg;
    private static final CacheName CACHENAME = new CacheName("testcache");
    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;
    private NamedCache versionCache;
    private NamedCache keyCache;
    private final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<MapEvent>(100);

    @BeforeClass
    public static void setSystemProperties() {
        System.setProperty("tangosol.pof.enabled", "true");
        System.setProperty("pof-config-file", "mvcc-pof-config-test.xml");
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty("tangosol.pof.enabled", "true");
        DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
        cmg = builder.setStorageEnabledCount(1).build();

        System.setProperty(SystemPropertyConst.DISTRIBUTED_LOCAL_STORAGE_KEY, "false");
        versionCache = CacheFactory.getCache(CACHENAME.getVersionCacheName());
        versionCache.addIndex(new MVCCExtractor(), false, null);
        keyCache = CacheFactory.getCache(CACHENAME.getKeyCacheName());

        events.clear();

    }

    private void addListener(IsolationLevel isolationLevel) {
        MapListener testMapListener = new MapListener() {
            @Override
            public void entryUpdated(MapEvent mapevent) {
                events.add(mapevent);
            }
            @Override
            public void entryInserted(MapEvent mapevent) {
                events.add(mapevent);
            }
            @Override
            public void entryDeleted(MapEvent mapevent) {
                events.add(mapevent);
            }
        };
        TransactionId tsevent = new TransactionId(BASETIME - 1, 0, 0);

        events.clear();

        versionCache.addMapListener(new MVCCMapListener<Integer, String>(testMapListener), 
                new MapEventTransformerFilter(AlwaysFilter.INSTANCE, new MVCCEventTransformer<Integer, String>(isolationLevel, tsevent, CACHENAME)), false);

    }

    @After
    public void tearDown() throws Exception {
        CacheFactory.shutdown();
        cmg.shutdownAll();
    }

    @Test
    public void testTransformInsert() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);

        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

    }

    @Test
    public void testTransformInsertUncommitted() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue, false);

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);

        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.open);

    }
    @Test
    public void testTransformIgnoreUncommitted() throws InterruptedException {

        addListener(readCommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue, false);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);

        assertNull(event);

    }

    @Test
    public void testTransformIgnoreBackdated() throws InterruptedException {

        addListener(readCommitted);

        TransactionId ts = new TransactionId(BASETIME - 2, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);

        MapEvent event = events.poll(1, TimeUnit.SECONDS);

        assertNull(event);

    }

    @Test
    public void testTransformUpdate() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);

        String testValue = "a test value";
        String testValue2 = "updated test value";

        put(99, ts, testValue);
        put(99, ts2, testValue2);

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts2, testValue, testValue2, MVCCCacheEvent.CommitStatus.commit);

    }
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

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts2, testValue, testValue2, MVCCCacheEvent.CommitStatus.open);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts3, testValue2, testValue3, MVCCCacheEvent.CommitStatus.commit);

    }

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

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

//      No event for ts2 - suppressed by readCommitted listener

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts3, testValue, testValue3, MVCCCacheEvent.CommitStatus.commit);

    }

    @Test
    public void testTransformDelete() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);
        remove(99, ts2);

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_DELETED, ts, testValue, null, MVCCCacheEvent.CommitStatus.commit);

    }

    @Test
    public void testTransformRollback() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);
        TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue, false);
        versionCache.remove(new VersionedKey<Integer>(99, ts));

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.open);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts2, testValue, null, MVCCCacheEvent.CommitStatus.rollback);

    }

    @Test
    public void testIgnoreVersionReap() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);
        versionCache.remove(new VersionedKey<Integer>(99, ts));

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        assertNull(event);

    }

    private void put(Integer key, TransactionId ts, String value) {
        put(key, ts, value, true);
    }

    private void put(Integer key, TransactionId ts, String value, boolean autocommit) {
        EntryProcessor insertProcessor = new ConditionalPut(AlwaysFilter.INSTANCE, value);
        EntryProcessor wrapper = new MVCCEntryProcessorWrapper<String, Object>(ts, insertProcessor, readUncommitted, autocommit, CACHENAME);
        keyCache.invoke(99, wrapper);
    }

    private void remove(Integer key, TransactionId ts) {
        EntryProcessor ep = new MVCCEntryProcessorWrapper<String, Object>(ts, new UnconditionalRemoveProcessor(), serializable, true, CACHENAME);
        keyCache.invoke(99, ep);
    }

    private void checkEvent(MVCCCacheEvent event, int expectedType, TransactionId ts, 
            String oldExpected, String newExpected, MVCCCacheEvent.CommitStatus commitstatus) {
        assertNotNull(event);
        assertEquals(99, event.getKey());
        assertEquals(newExpected, event.getNewValue());
        assertEquals(oldExpected, event.getOldValue());
        assertEquals(expectedType, event.getId());
        assertEquals(commitstatus, event.getCommitStatus());

    }

}
