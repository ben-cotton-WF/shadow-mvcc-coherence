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

import static com.shadowmvcc.coherence.domain.IsolationLevel.repeatableRead;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.domain.SampleDomainObject;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.filter.EqualsFilter;

/**
 * Test that distributed invocation works correctly when a member dies.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCTransactionalCacheKillMemberTest extends AbstractLittlegridTest {

    private static final String TESTCACHEMAME = "testcache";
    private MVCCTransactionalCacheImpl<Integer, SampleDomainObject> cache;

    /**
     * create cluster and initialise cache.
     */
    @Before
    public void setUp() {
        System.out.println("******initialise cache");
        cache = new MVCCTransactionalCacheImpl<Integer, SampleDomainObject>(TESTCACHEMAME, "InvocationService");
    }

    /**
     * invoke a long-running entryprocessor on all members,
     * then kill a member before it completes. Check that the
     * invocation still produces the correct result
     */
    @Test(timeout = 20000)
    public void testInvokeAllFilter() {
        System.out.println("******InvokeAll(Filter)");

        final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
        final TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);

        SampleDomainObject val1 = new SampleDomainObject(88, "eighty-eight");
        SampleDomainObject val2 = new SampleDomainObject(77, "seventy-seven");

        for (int key = 0; key < 5; key++) {
            cache.insert(ts1, true, key * 2, val1);
            cache.insert(ts1, true, key * 2 + 1, val2);
        }

        Filter filter = new EqualsFilter(new PofExtractor(null, SampleDomainObject.POF_INTV), 77);
        EntryProcessor ep = new LongWaitingEntryProcessor();

        asynchMemberKill(1);

        Set<Integer> keySet = cache.invokeAll(ts2, repeatableRead, false, true, filter, ep).getResultMap().keySet();

        Set<Integer> expected = new HashSet<Integer>(5);
        expected.add(1);
        expected.add(3);
        expected.add(5);
        expected.add(7);
        expected.add(9);

        assertEquals(5, keySet.size());
        assertTrue(keySet.containsAll(expected));

        for (Integer key : expected) {
            assertEquals(val2, cache.get(ts2, repeatableRead, key));
        }

    }


    /**
     * Spawn a thread to kill a member.  
     * @param memberId the member to kill
     */
    private void asynchMemberKill(final int memberId) {
        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("interrupted");
                }
                getClusterMemberGroup().getClusterMember(memberId).stop();
            }
        }).start();
    }

}
