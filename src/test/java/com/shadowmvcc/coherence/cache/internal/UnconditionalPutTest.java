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
import static junit.framework.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.SampleDomainObject;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.invocable.MVCCEntryProcessorWrapper;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Test the UnconditionalPutProcessor.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class UnconditionalPutTest extends AbstractLittlegridTest {

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
     * Do the test.
     */
    @Test
    public void testPut() {

        final TransactionId ts = new TransactionId(BASETIME, 0, 0);
        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        CacheName cacheName = new CacheName(TESTCACHEMAME);
        NamedCache keyCache = CacheFactory.getCache(cacheName.getKeyCacheName());
        NamedCache versionCache = CacheFactory.getCache(cacheName.getVersionCacheName());

        keyCache.invoke(theKey, 
                new MVCCEntryProcessorWrapper<Integer, String>(
                        ts, new UnconditionalPutProcessor(theValue, true), readCommitted, true, cacheName));

        assertEquals(theValue, versionCache.get(new VersionedKey<Integer>(99, ts)));

    }

}
