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
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.util.concurrent.Semaphore;

import org.junit.Test;

import com.shadowmvcc.coherence.domain.SampleDomainObject;
import com.tangosol.io.pof.PortableException;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.extractor.PofUpdater;
import com.tangosol.util.processor.ExtractorProcessor;
import com.tangosol.util.processor.UpdaterProcessor;

/**
 * Test the various methods of MVCCTransactionalCacheImpl. The intent is to provide
 * comprehensive coverage of the methods and associated entryprocessors, invocables etc
 * including committed/uncommitted reads, operations where real-time sequence
 * differs from transaction-time sequence etc.
 * 
 * This is really a fairly comprehensive integration test of the middle and low level
 * functionality.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCTransactionalCacheKeyTest extends AbstractMVCCTransactionalCacheTest {

    /**
     * Test putting a value uncommitted, then reading it waits for the commit.
     */
    @Test
    public void testPutCommitRead() {

        System.out.println("******PutCommitRead");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        assertNull(cache.put(ts1, repeatableRead, false, theKey, theValue));

        asynchCommit(ts1, theKey);

        assertEquals(theValue, cache.get(ts2, repeatableRead, theKey));

    }

    /**
     * Test the containsKey method.
     */
    @Test
    public void testContainsKey() {

        System.out.println("******ContainsKey");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
        Integer otherKey = 98;

        assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
        assertNull(cache.put(ts3, repeatableRead, true, otherKey, theValue));


        assertTrue(cache.containsKey(ts2, repeatableRead, theKey));
        assertFalse(cache.containsKey(ts2, repeatableRead, otherKey));
        assertFalse(cache.containsKey(ts2, repeatableRead, 97));
    }

    /**
     * Test that reading a removed value (i.e. a delete marker)
     * works correctly.
     */
    @Test
    public void testPutRemoveRead() {

        System.out.println("******PutRemoveRead");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
        cache.remove(ts2, repeatableRead, true, theKey);

        assertNull(cache.get(ts3, readCommitted, theKey));

    }
    
    /**
     * Test that reading a removed value works correctly if the 
     * remove is initially uncommitted.
     */
    @Test
    public void testPutRemoveReadCommit() {

        System.out.println("******PutRemoveReadCommit");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
        cache.remove(ts2, repeatableRead, false, theKey);

        Semaphore flag = new Semaphore(0);
        asynchCommit(flag, ts2, theKey);

        assertNull(cache.get(ts3, readUncommitted, theKey));

        flag.release();

        assertNull(cache.get(ts3, readCommitted, theKey));

    }

    /**
     * Test reading a value that is removed, then the remove is rolled back.
     */
    @Test
    public void testPutRemoveReadRollback() {

        System.out.println("******PutRemoveRead");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
        cache.remove(ts2, repeatableRead, false, theKey);

        asynchRollback(ts2, theKey);

        assertEquals(theValue, cache.get(ts3, readCommitted, theKey));

    }

    /**
     * Test reading a value that is inserted, then the insert rolled back.
     */
    @Test
    public void testInsertRollbackRead() {

        System.out.println("******InsertRollbackRead");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        assertNull(cache.put(ts1, repeatableRead, false, theKey, theValue));

        asynchRollback(ts1, theKey);

        assertNull(cache.get(ts2, repeatableRead, theKey));

    }

    /**
     * Test that inserting a value with a timestamp earlier than a read version
     * fails.
     */
    @Test(expected = PortableException.class)
    public void testPutEarlierPut() {

        System.out.println("******PutEarlierPut");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        assertNull(cache.put(ts2, repeatableRead, true, theKey, theValue));

        SampleDomainObject earliervalue = new SampleDomainObject(77, "seventy-seven");
        cache.put(ts1, repeatableRead, true, theKey, earliervalue);
    }

    /**
     * Test that inserting a value earlier than an existing value succeeds when
     * the later value has not been read.
     */
    @Test
    public void testInsertEarlierPut() {

        System.out.println("******InsertEarlierPut");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        cache.insert(ts2, true, theKey, theValue);

        SampleDomainObject earliervalue = new SampleDomainObject(77, "seventy-seven");
        assertNull(cache.put(ts1, repeatableRead, true, theKey, earliervalue));
    }

    /**
     * Test execution of a read only EntryProcessor.
     */
    @Test
    public void testInvoke() {

        System.out.println("******Invoke");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        cache.insert(ts1, true, theKey, theValue);

        EntryProcessor ep = new ExtractorProcessor(new PofExtractor(null, SampleDomainObject.POF_INTV));
        InvocationFinalResult<Integer, Integer> ifr = cache.invoke(ts2, repeatableRead, true, true, theKey, ep);

        assertEquals(88, (int) ifr.getResultMap().get(theKey));

    }

    /**
     * Test execution of a read write EntryProcessor.
     */
    @Test
    public void testInvokeReadWrite() {

        System.out.println("******Invoke");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        cache.insert(ts1, true, theKey, theValue);

        EntryProcessor ep = new UpdaterProcessor(new PofUpdater(SampleDomainObject.POF_INTV), 42);

        cache.invoke(ts2, repeatableRead, true, false, theKey, ep);
        
        assertEquals(42, ((SampleDomainObject) cache.get(ts2, readCommitted, theKey)).getIntValue());

    }
    
    /**
     * Test execution of a read write EntryProcessor executed read-only.
     */
    @Test(expected = PortableException.class)
    public void testInvokeFailReadonly() {

        System.out.println("******Invoke");

        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");

        cache.insert(ts1, true, theKey, theValue);

        EntryProcessor ep = new UpdaterProcessor(new PofUpdater(SampleDomainObject.POF_INTV), 42);

        cache.invoke(ts2, repeatableRead, true, true, theKey, ep);

    }

    /**
     * Test the containsValue method.
     */
    @Test
    public void testContainsValue() {
    
        System.out.println("******ContainsValue");
    
        Integer theKey = 99;
        SampleDomainObject theValue = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject otherValue = new SampleDomainObject(99, "ninety-nine");
        SampleDomainObject noValue = new SampleDomainObject(77, "seventy-seven");
        Integer otherKey = 98;
    
        assertNull(cache.put(ts1, repeatableRead, true, theKey, theValue));
        assertNull(cache.put(ts3, repeatableRead, true, otherKey, otherValue));
    
        assertTrue(cache.containsValue(ts2, repeatableRead, theValue));
        assertFalse(cache.containsValue(ts2, repeatableRead, otherValue));
        assertFalse(cache.containsValue(ts2, repeatableRead, noValue));
    }


}
