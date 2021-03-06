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

package com.shadowmvcc.coherence.cache.internal;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;
import static com.shadowmvcc.coherence.domain.IsolationLevel.readUncommitted;
import static com.shadowmvcc.coherence.domain.IsolationLevel.repeatableRead;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.shadowmvcc.coherence.domain.SampleDomainObject;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.aggregator.LongSum;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.extractor.PofUpdater;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.processor.UpdaterProcessor;

/**
 * Test the various methods of MVCCTransactionalCacheImpl. The intent is to provide
 * comprehensive coverage of the methods and associated entryprocessors, invocables etc
 * including committed/uncommitted reads, operations where real-time sequence
 * differs from transaction-time sequence etc.
 * 
 * This is really a fairly comprehensive integration test of the middle and low level
 * functionality for the set-based operations.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCTransactionalCacheSetTest extends AbstractMVCCTransactionalCacheTest {

    /**
     * Test that the size() method returns the correct value for
     * the timestamp at which it is invoked.
     * @throws Throwable 
     */
    @Test
    public void testSize() throws Throwable {

        System.out.println("******Size");

        SampleDomainObject val2 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val4 = new SampleDomainObject(88, "eighty-eight");

        for (int key = 0; key < 3; key++) {
            cache.insert(ts2, true, key, val2);
        }

        for (int key = 0; key < 5; key++) {
            cache.insert(ts4, true, key, val4);
        }

        assertEquals(0, cache.size(ts1, repeatableRead));
        assertEquals(3, cache.size(ts3, repeatableRead));
        assertEquals(5, cache.size(ts5, repeatableRead));

        cache.insert(ts4, false, 6, val4);

        assertEquals(6, cache.size(ts5, readUncommitted));

        asynchCommit(ts4, 6);

        assertEquals(6, cache.size(ts5, repeatableRead));

        cache.insert(ts4, false, 7, val4);

        assertEquals(7, cache.size(ts5, readUncommitted));

        asynchRollback(ts4, 7);

        assertEquals(6, cache.size(ts5, repeatableRead));

        cache.remove(ts6, repeatableRead, true, 0);

        assertEquals(5, cache.size(ts7, repeatableRead));
    }

    /**
     * Test that entrySet with a filter returns the correct values for its timestamp.
     * @throws Throwable 
     */
    @Test
    public void testEntrySet() throws Throwable {
        System.out.println("******EntrySet");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts1, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);

        Set<Map.Entry<Integer, SampleDomainObject>> entrySet = cache.entrySet(ts2, repeatableRead, filter);

        Map<Integer, SampleDomainObject> expected = new HashMap<Integer, SampleDomainObject>(5);
        expected.put(1, val2);
        expected.put(3, val2);
        expected.put(5, val2);
        expected.put(7, val2);
        expected.put(9, val2);

        assertEquals(5, entrySet.size());
        assertTrue(entrySet.containsAll(expected.entrySet()));

    }

    /**
     * Test that entrySet without a filter returns the correct values for its timestamp.
     * @throws Throwable 
     */
    @Test
    public void testEntrySetAll() throws Throwable {
        System.out.println("******EntrySetAll");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts3, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Set<Map.Entry<Integer, SampleDomainObject>> entrySet = cache.entrySet(ts2, repeatableRead);

        Map<Integer, SampleDomainObject> expected = new HashMap<Integer, SampleDomainObject>(5);
        expected.put(1, val2);
        expected.put(3, val2);
        expected.put(5, val2);
        expected.put(7, val2);
        expected.put(9, val2);

        assertEquals(5, entrySet.size());
        assertTrue(entrySet.containsAll(expected.entrySet()));

    }

    /**
     * Test that entrySet correctly waits for uncommitted changes
     * and returns the right result with rollbacks and commits.
     * @throws Throwable 
     */
    @Test
    public void testEntrySetWithUncommitted() throws Throwable {
        System.out.println("******EntrySet");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts1, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        cache.insert(ts1, true, 10, val1);
        cache.insert(ts1, true, 11, val2);
        cache.insert(ts1, true, 12, val1);
        cache.insert(ts1, true, 13, val2);

        cache.insert(ts2, false, 10, val2);
        cache.insert(ts2, false, 11, val1);
        cache.insert(ts2, false, 12, val2);
        cache.insert(ts2, false, 13, val1);

        Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);

        asynchCommit(ts2, 10);
        asynchCommit(ts2, 11);
        asynchRollback(ts2, 12);
        asynchRollback(ts2, 13);
        Set<Map.Entry<Integer, SampleDomainObject>> entrySet = cache.entrySet(ts3, repeatableRead, filter);

        Map<Integer, SampleDomainObject> expected = new HashMap<Integer, SampleDomainObject>(5);
        expected.put(1, val2);
        expected.put(3, val2);
        expected.put(5, val2);
        expected.put(7, val2);
        expected.put(9, val2);
        expected.put(10, val2);
        expected.put(13, val2);

        assertEquals(expected.size(), entrySet.size());
        assertTrue(entrySet.containsAll(expected.entrySet()));

    }

    /**
     * Test keySet with a filter.
     * @throws Throwable 
     */
    @Test
    public void testKeySet() throws Throwable {
        System.out.println("******KeySet");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts1, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);

        Set<Integer> keySet = cache.keySet(ts2, repeatableRead, filter);

        Set<Integer> expected = new HashSet<Integer>(5);
        expected.add(1);
        expected.add(3);
        expected.add(5);
        expected.add(7);
        expected.add(9);

        assertEquals(5, keySet.size());
        assertTrue(keySet.containsAll(expected));

    }

    /**
     * test keySet without a filter.
     * @throws Throwable 
     */
    @Test
    public void testKeySetAll() throws Throwable {
        System.out.println("******KeySetAll");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts3, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Set<Integer> keySet = cache.keySet(ts2, repeatableRead);

        Set<Integer> expected = new HashSet<Integer>(5);
        expected.add(1);
        expected.add(3);
        expected.add(5);
        expected.add(7);
        expected.add(9);

        assertEquals(5, keySet.size());
        assertTrue(keySet.containsAll(expected));

    }

    /**
     * Test invokeAll with a filter.
     * @throws Throwable 
     */
    @Test
    public void testInvokeAllFilter() throws Throwable {
        System.out.println("******InvokeAll(Filter)");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts1, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);
        EntryProcessor ep = new UpdaterProcessor(new PofUpdater(SampleDomainObject.POF_STRV), "seventy-eight");

        Set<Integer> keySet = cache.invokeAll(ts2, repeatableRead, false, false, filter, ep).getResultMap().keySet();

        Set<Integer> expected = new HashSet<Integer>(5);
        expected.add(1);
        expected.add(3);
        expected.add(5);
        expected.add(7);
        expected.add(9);

        assertEquals(5, keySet.size());
        assertTrue(keySet.containsAll(expected));

        SampleDomainObject expectedObject = new SampleDomainObject(77, "seventy-eight");

        for (Integer key : expected) {
            assertEquals(expectedObject, cache.get(ts2, readUncommitted, key));
        }

    }
    
    
    /**
     * Test invokeAll with a collection of keys.
     */
    @Test
    public void testInvokeAllKeys() {
        System.out.println("******InvokeAll(Keys)");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 4; key++) {
            cache.insert(ts1, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }
        cache.insert(ts1, false, 8, val1);
        cache.insert(ts1, false, 9, val2);

        asynchCommit(ts1, 8);
        asynchCommit(ts1, 9);

        EntryProcessor ep = new UpdaterProcessor(new PofUpdater(SampleDomainObject.POF_STRV), "seventy-eight");

        Set<Integer> expected = new HashSet<Integer>(5);
        expected.add(1);
        expected.add(3);
        expected.add(5);
        expected.add(7);
        expected.add(9);

        Set<Integer> keySet = cache.invokeAll(ts2, repeatableRead, false, false, expected, ep).getResultMap().keySet();


        assertEquals(5, keySet.size());
        assertTrue(keySet.containsAll(expected));

        SampleDomainObject expectedObject = new SampleDomainObject(77, "seventy-eight");

        for (Integer key : expected) {
            assertEquals(expectedObject, cache.get(ts2, readUncommitted, key));
        }

    }

    /**
     * Test getAll.
     */
    @Test
    public void testGetAll() {
        System.out.println("******GetAll");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts1, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Set<Integer> keys = new HashSet<Integer>(5);
        keys.add(1);
        keys.add(3);
        keys.add(5);
        keys.add(7);
        keys.add(11);

        Map<Integer, SampleDomainObject> results = cache.getAll(ts2, repeatableRead, keys);

        Map<Integer, SampleDomainObject> expected = new HashMap<Integer, SampleDomainObject>(4);
        expected.put(1, val2);
        expected.put(3, val2);
        expected.put(5, val2);
        expected.put(7, val2);

        assertEquals(4, results.size());
        assertTrue(results.entrySet().containsAll(expected.entrySet()));

    }

    /**
     * Test putAll.
     * @throws Throwable 
     */
    @Test
    public void testPutAll() throws Throwable {

        System.out.println("******PutAll");

        Map<Integer, SampleDomainObject> valueMap = new HashMap<Integer, SampleDomainObject>();
        for (Integer theKey = 0; theKey < 10; theKey++) {
            valueMap.put(theKey, new SampleDomainObject(theKey, "eighty-eight"));
        }

        cache.putAll(ts1, true, valueMap);

        assertEquals(10, cache.size(ts2, readCommitted));

        assertTrue(cache.entrySet(ts2, readCommitted).containsAll(valueMap.entrySet()));


    }

    /**
     * Test that clear() works correctly. Creates deleted events for all extant entries
     * @throws Throwable 
     */
    @Test
    public void testClear() throws Throwable {

        System.out.println("******Clear");

        SampleDomainObject firstTranche = new SampleDomainObject(1, "first tranche");
        SampleDomainObject secondTranche = new SampleDomainObject(2, "second tranche");
        Map<Integer, SampleDomainObject> expected = new HashMap<Integer, SampleDomainObject>();
        for (Integer theKey = 0; theKey < 10; theKey++) {
            cache.insert(ts1, true, theKey, firstTranche);
            cache.insert(ts4, true, theKey + 10, secondTranche);
            expected.put(theKey + 10, secondTranche);
        }

        cache.clear(ts2, true);

        assertEquals(0, cache.size(ts3, readCommitted));
        assertEquals(10, cache.size(ts5, readCommitted));

        assertTrue(cache.entrySet(ts5, readCommitted).containsAll(expected.entrySet()));

    }

    /**
     * Test the values() method.
     * @throws Throwable 
     */
    @Test
    public void testValues() throws Throwable {
        System.out.println("******Values");

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts3, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Collection<SampleDomainObject> values = cache.values(ts2, repeatableRead);

        Map<Integer, SampleDomainObject> expected = new HashMap<Integer, SampleDomainObject>(5);
        expected.put(1, val2);
        expected.put(3, val2);
        expected.put(5, val2);
        expected.put(7, val2);
        expected.put(9, val2);

        assertEquals(5, values.size());
        for (SampleDomainObject value : values) {
            assertEquals(val2, value);
        }

    }

    /**
     * Test aggregation over a filter.
     * @throws Throwable 
     */
    @Test
    public void testAggregateFilter() throws Throwable {

        System.out.println("******AggregateFilter");

        SampleDomainObject val2 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val4 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 3; key++) {
            cache.insert(ts2, true, key, val2);
        }

        for (int key = 0; key < 5; key++) {
            cache.insert(ts2, true, key + 10, val4);
        }

        assertEquals(3, 
                cache.aggregate(ts3, repeatableRead, new EqualsFilter(
                        new PofExtractor(null, SampleDomainObject.POF_INTV), 88), new Count()));
    }

    /**
     * Test aggregation over a collection of keys.
     * @throws Throwable 
     */
    @Test
    public void testAggregateKeys() throws Throwable {

        System.out.println("******AggregateKeys");

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
                cache.aggregate(ts3, repeatableRead,
                        keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));

        cache.insert(ts2, false, 20, val4);

        assertEquals(Long.valueOf(88 + 77 + 77 + 77), 
                cache.aggregate(ts3, readUncommitted,
                        keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));

        asynchCommit(ts2, 20);

        assertEquals(Long.valueOf(88 + 77 + 77 + 77), 
                cache.aggregate(ts3, readCommitted,
                        keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));

        assertEquals(Long.valueOf(88 + 77 + 77 + 77), 
                cache.aggregate(ts3, repeatableRead,
                        keys, new LongSum(new PofExtractor(null, SampleDomainObject.POF_INTV))));
    }


}
