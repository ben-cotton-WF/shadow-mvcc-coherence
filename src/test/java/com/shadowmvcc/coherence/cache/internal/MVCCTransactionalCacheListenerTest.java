package com.shadowmvcc.coherence.cache.internal;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;
import static com.shadowmvcc.coherence.event.MVCCCacheEvent.CommitStatus.commit;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.domain.SampleDomainObject;
import com.shadowmvcc.coherence.event.MVCCCacheEvent;
import com.shadowmvcc.coherence.event.MVCCCacheEvent.CommitStatus;
import com.tangosol.util.Filter;
import com.tangosol.util.MapEvent;
import com.tangosol.util.MapListener;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.filter.EqualsFilter;

/**
 * Test the map listener methods on MVCCTransactionalCacheImpl.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 * 
 * 
 *
 */
public class MVCCTransactionalCacheListenerTest extends
        AbstractMVCCTransactionalCacheTest {
    
    private final BlockingQueue<MVCCCacheEvent> eventQueue = new ArrayBlockingQueue<MVCCCacheEvent>(10);
    
    /**
     * Test map listener, just pop the events on a queue.
     *
     */
    private class TestListener implements MapListener {
        @Override
        public void entryInserted(final MapEvent mapevent) {
            eventQueue.add((MVCCCacheEvent) mapevent);
        }
        @Override
        public void entryUpdated(final MapEvent mapevent) {
            eventQueue.add((MVCCCacheEvent) mapevent);
        }
        @Override
        public void entryDeleted(final MapEvent mapevent) {
            eventQueue.add((MVCCCacheEvent) mapevent);
        }
    }
    
    private static final String MATCHSTRING = "YES";
    private static final String NOMATCHSTRING = "NO";
    /**
     * make sure the queue is empty.
     */
    @Before
    public void initialise() {
        eventQueue.clear();
    }
    
    /**
     * Test we get an insert event.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testInsertEventCacheListener() throws InterruptedException {
        
        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted);
        
        SampleDomainObject expected = new SampleDomainObject(99, "test 1");
        
        cache.put(ts3, readCommitted, true, 99, expected);
        
        MVCCCacheEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
        
        assertEvent(event, 99, expected, null, commit, MapEvent.ENTRY_INSERTED);
    }
    
    /**
     * Test removing a listener.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testRemoveCacheListener() throws InterruptedException {
        
        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted);
        
        SampleDomainObject expected = new SampleDomainObject(99, "test 1");
        
        cache.put(ts3, readCommitted, true, 99, expected);
        
        assertTrue(eventQueue.poll(1, TimeUnit.SECONDS) != null);

        cache.removeMapListener(testListener);

        cache.put(ts3, readCommitted, true, 99, expected);

        assertNull(eventQueue.poll(1, TimeUnit.SECONDS));
    }
    
    /**
     * Test we get an update event.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testUpdateEvent() throws InterruptedException {
        
        SampleDomainObject oldValue = new SampleDomainObject(1, "test 1");
        
        cache.put(ts1, readCommitted, true, 99, oldValue);

        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted);

        SampleDomainObject newValue = new SampleDomainObject(2, "test 2");

        cache.put(ts3, readCommitted, true, 99, newValue);
        MVCCCacheEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
        
        assertEvent(event, 99, newValue, oldValue, commit, MapEvent.ENTRY_UPDATED);
    }
    /**
     * Test we get an delet event.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testDeleteEvent() throws InterruptedException {
        
        SampleDomainObject oldValue = new SampleDomainObject(1, "test 1");
        
        cache.put(ts1, readCommitted, true, 99, oldValue);

        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted);

        cache.remove(ts3, readCommitted, true, 99);
        
        MVCCCacheEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
        
        assertEvent(event, 99, null, oldValue, commit, MapEvent.ENTRY_DELETED);
    }

    /**
     * Test we get an insert event.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testInsertEventFilterListenerMatch() throws InterruptedException {
        
        Filter filter = new EqualsFilter(new PofExtractor(String.class, SampleDomainObject.POF_STRV), MATCHSTRING);
        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted, filter, false);
        
        SampleDomainObject expected = new SampleDomainObject(99, MATCHSTRING);
        
        cache.put(ts3, readCommitted, true, 99, expected);
        
        MVCCCacheEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
        
        assertEvent(event, 99, expected, null, commit, MapEvent.ENTRY_INSERTED);
    }
    
    /**
     * Test we don't get an insert event.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testInsertEventFilterListenerNoMatch() throws InterruptedException {
        
        Filter filter = new EqualsFilter(new PofExtractor(String.class, SampleDomainObject.POF_STRV), MATCHSTRING);
        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted, filter, false);
        
        SampleDomainObject expected = new SampleDomainObject(99, NOMATCHSTRING);
        
        cache.put(ts3, readCommitted, true, 99, expected);
        
        assertNull(eventQueue.poll(1, TimeUnit.SECONDS));

    }
    
    /**
     * Test we get an event when an update takes a value out.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testInsertEventFilterUpdateOut() throws InterruptedException {
        
        SampleDomainObject firstVersion = new SampleDomainObject(99, MATCHSTRING);
        SampleDomainObject secondVersion = new SampleDomainObject(99, NOMATCHSTRING);

        cache.put(ts1, readCommitted, true, 99, firstVersion);
        
        Filter filter = new EqualsFilter(new PofExtractor(String.class, SampleDomainObject.POF_STRV), MATCHSTRING);
        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted, filter, false);
        
        cache.put(ts3, readCommitted, true, 99, secondVersion);

        MVCCCacheEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
        
        assertEvent(event, 99, secondVersion, firstVersion, commit, MapEvent.ENTRY_UPDATED);

    }
    /**
     * Test we get an event when an update takes a value out.
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testInsertEventFilterDelete() throws InterruptedException {
        
        SampleDomainObject firstVersion = new SampleDomainObject(99, MATCHSTRING);

        cache.put(ts1, readCommitted, true, 99, firstVersion);
        
        Filter filter = new EqualsFilter(new PofExtractor(String.class, SampleDomainObject.POF_STRV), MATCHSTRING);
        TestListener testListener = new TestListener();
        
        cache.addMapListener(testListener, ts2, readCommitted, filter, false);
        
        cache.remove(ts3, readCommitted, true, 99);

        MVCCCacheEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
        
        assertEvent(event, 99, null, firstVersion, commit, MapEvent.ENTRY_DELETED);

    }

    
    /**
     * Check the event is what we expect.
     * @param event the event
     * @param key expected key
     * @param newValue expected new value
     * @param oldValue expected old value
     * @param committed commit status
     * @param eventType insert, update, delete
     */
    private void assertEvent(final MVCCCacheEvent event, final Integer key, final SampleDomainObject newValue,
            final SampleDomainObject oldValue, final CommitStatus committed, final int eventType) {

        assertEquals(newValue, event.getNewValue());
        assertEquals(key, event.getKey());
        assertEquals(oldValue, event.getOldValue());
        assertEquals(committed, event.getCommitStatus());
        assertEquals(eventType, event.getId());
    }

}
