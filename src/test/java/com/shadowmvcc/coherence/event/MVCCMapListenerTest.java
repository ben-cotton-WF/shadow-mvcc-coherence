/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

package com.shadowmvcc.coherence.event;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;
import static com.shadowmvcc.coherence.domain.IsolationLevel.readUncommitted;
import static com.shadowmvcc.coherence.domain.IsolationLevel.serializable;
import static com.tangosol.util.MapEvent.ENTRY_DELETED;
import static com.tangosol.util.MapEvent.ENTRY_INSERTED;
import static com.tangosol.util.MapEvent.ENTRY_UPDATED;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.internal.UnconditionalRemoveProcessor;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.shadowmvcc.coherence.invocable.MVCCEntryProcessorWrapper;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.MapEventTransformerFilter;
import com.tangosol.util.processor.ConditionalPut;

/**
 * test the MVCC map listener.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCMapListenerTest extends AbstractLittlegridTest {

    private static final CacheName CACHENAME = new CacheName("testcache");
    private NamedCache versionCache;
    private NamedCache keyCache;
    private final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<MapEvent>(100);

    /**
     * create cluster and initialise cache.
     */
    @Before
    public void setUp() {

        versionCache = CacheFactory.getCache(CACHENAME.getVersionCacheName());
        versionCache.addIndex(new MVCCExtractor(), false, null);
        keyCache = CacheFactory.getCache(CACHENAME.getKeyCacheName());

        events.clear();

    }

    /**
     * Add the listener.
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

        versionCache.addMapListener(new MVCCMapListener<Integer, String>(testMapListener), 
                new MapEventTransformerFilter(AlwaysFilter.INSTANCE, new MVCCEventTransformer<Integer, String>(
                        isolationLevel, tsevent, CACHENAME)), false);

    }

    /**
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testTransformInsert() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue);

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);

        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

    }

    /**
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testTransformInsertUncommitted() throws InterruptedException {

        addListener(readUncommitted);

        TransactionId ts = new TransactionId(BASETIME, 0, 0);

        String testValue = "a test value";

        put(99, ts, testValue, false);

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);

        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.open);

    }
    /**
     * @throws InterruptedException if interrupted
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
     * @throws InterruptedException if interrupted
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
     * @throws InterruptedException if interrupted
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

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts2, testValue, testValue2, MVCCCacheEvent.CommitStatus.commit);

    }
    /**
     * @throws InterruptedException if interrupted
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

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts2, testValue, testValue2, MVCCCacheEvent.CommitStatus.open);

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts3, testValue2, testValue3, MVCCCacheEvent.CommitStatus.commit);

    }

    /**
     * @throws InterruptedException if interrupted
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

        MVCCCacheEvent event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_INSERTED, ts, null, testValue, MVCCCacheEvent.CommitStatus.commit);

//      No event for ts2 - suppressed by readCommitted listener

        event = (MVCCCacheEvent) events.poll(1, TimeUnit.SECONDS);
        checkEvent(event, ENTRY_UPDATED, ts3, testValue, testValue3, MVCCCacheEvent.CommitStatus.commit);

    }

    /**
     * @throws InterruptedException if interrupted
     */
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

    /**
     * @throws InterruptedException if interrupted
     */
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

    /**
     * @throws InterruptedException if interrupted
     */
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
    private void put(final Integer key, final TransactionId ts, final String value, final boolean autocommit) {
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
     * @param commitstatus committed
     */
    private void checkEvent(final MVCCCacheEvent event, final int expectedType, final TransactionId ts, 
            final String oldExpected, final String newExpected, final MVCCCacheEvent.CommitStatus commitstatus) {
        assertNotNull(event);
        assertEquals(99, event.getKey());
        assertEquals(newExpected, event.getNewValue());
        assertEquals(oldExpected, event.getOldValue());
        assertEquals(expectedType, event.getId());
        assertEquals(commitstatus, event.getCommitStatus());

    }

}
