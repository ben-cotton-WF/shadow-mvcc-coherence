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

package com.shadowmvcc.coherence.invocable;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
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
