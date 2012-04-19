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

package com.shadowmvcc.coherence.index;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Test the MVCCSurfaceFilter with a littlegrid.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCSnapshotPurgeFilterTest extends AbstractLittlegridTest {

    private static final String TESTCACHENAME = "testcache";
    private NamedCache testCache;
    
    private static final int COUNTI = 20;
    private static final int COUNTJ = 20;
    private static final int IOFFSET = 1000;
    private static final int JOFFSET = 500;

    /**
     * initialise cache.
     */
    @Before
    public void setUp() {
        System.out.println("***MVCCSnapshotPurgeFilterTest initialise");
        testCache = CacheFactory.getCache(TESTCACHENAME);
        testCache.addIndex(new MVCCExtractor(), false, null);
        testCache.addIndex(new PofExtractor(null,
                new SimplePofPath(VersionedKey.POF_LOGICALKEY), AbstractExtractor.KEY), false, null);
    }

    /**
     * Check that we get all but the latest within the range.
     */
    @Test
    public void testRange() {

        System.out.println("***MVCCSnapshotPurgeFilterTest testRange");
        Set<VersionedKey<Integer>> expected = new HashSet<VersionedKey<Integer>>();
        
        final long rangeEnd = BASETIME + 10500;
        final long rangeStart = BASETIME + 1500;
        
        for (int i = 0; i < COUNTI; i++) {
            for (int j = 0; j < COUNTJ; j++) {
                putTestValue(testCache, i, BASETIME + (i * IOFFSET) + (j * JOFFSET), "test value");
            }
        }
        
        for (int i = 0; i < COUNTI; i++) {
            VersionedKey<Integer> lastInRange = null;
            for (int j = 0; j < COUNTJ; j++) {
                long time = BASETIME + (i * IOFFSET) + (j * JOFFSET);
                if (time > rangeStart && time <= rangeEnd) {
                    VersionedKey<Integer> vkey = makeKey(i, time);
                    expected.add(vkey);
                    lastInRange = vkey;
                }
            }
            if (lastInRange != null) {
                expected.remove(lastInRange);
            }
        }
        Filter pfilter = new MVCCSnapshotPurgeFilter<Object>(
                new TransactionId(rangeStart, 0, 0), new TransactionId(rangeEnd, 0, 0));
        
        @SuppressWarnings("unchecked")
        Set<VersionedKey<Integer>> result = (Set<VersionedKey<Integer>>) testCache.keySet(pfilter);

        System.out.println("total entries: " + testCache.size());
        System.out.println("matching entries: " + expected.size());
        
        Assert.assertEquals(expected.size(), result.size());
        Assert.assertTrue(result.containsAll(expected));
        Assert.assertTrue(expected.containsAll(result));

    }

    /**
     * Put a test value in the version cache.
     * @param cache cache
     * @param key key
     * @param timestamp transaction id
     * @param value value
     */
    @SuppressWarnings("unchecked")
    private void putTestValue(@SuppressWarnings("rawtypes") final Map cache, final int key,
            final long timestamp, final String value) {
        cache.put(makeKey(key, timestamp), value);
    }
    
    /**
     * Make a versioned key.
     * @param key logical key
     * @param timestamp timestamp
     * @return the versioned key
     */
    private VersionedKey<Integer> makeKey(final int key, final long timestamp) {
        return new VersionedKey<Integer>(key, new TransactionId(timestamp, 0, 0));
    }
    
}
