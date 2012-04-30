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

package com.shadowmvcc.coherence.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

/**
 * Test serialisation of domain objects.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class DomainSerialisationTest {

    private ConfigurablePofContext pofContext;
    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;

    /**
     * initialise POF context.
     */
    @Before
    public void setUp() {
        pofContext = new ConfigurablePofContext("mvcc-pof-config-test.xml");
    }

    /**
     * TransactionId.
     */
    @Test
    public void testTransactionId() {
        TransactionId vo = new TransactionId(BASETIME, 123, 456);
        assertPofFidelity(vo);
    }

    /**
     * VersionedKey.
     */
    @Test
    public void testVersionedKey() {
        VersionedKey<Integer> vo = new VersionedKey<Integer>(982, new TransactionId(BASETIME + 17, 124, 457));
        assertPofFidelity(vo);
    }

    /**
     * SampleDomainObject.
     */
    @Test
    public void testSampleDomainObject() {
        SampleDomainObject vo = new SampleDomainObject(123, "456");
        assertPofFidelity(vo);
    }

    /**
     * TransactionSetWrapper.
     */
    @Test
    public void testTransactionSetWrapper() {
        TransactionSetWrapper tsw = new TransactionSetWrapper();
        NavigableSet<TransactionId> ts = new TreeSet<TransactionId>();
        ts.add(new TransactionId(BASETIME + 17, 124, 457));
        ts.add(new TransactionId(BASETIME + 42, 124, 457));
        ts.add(new TransactionId(BASETIME + 42, 124, 455));
        tsw.setTransactionIdSet(ts);
        assertPofFidelityByReflection(tsw);
    }

    /**
     * Processor result with retry key.
     */
    public void testProcessorResult1() {
        TransactionId ts = new TransactionId(BASETIME + 17, 124, 457);
        ProcessorResult<String, Integer> pr = new ProcessorResult<String, Integer>(
                new CacheName("humpty-dumpty"), new VersionedKey<String>("ABC", ts));
        assertPofFidelityByReflection(pr);
    }
    /**
     * Processor result with return value.
     */
    public void testProcessorResult2() {
        ProcessorResult<String, Integer> pr = new ProcessorResult<String, Integer>(99, true, false);
        assertPofFidelityByReflection(pr);
    }


    /**
     * Check using equals.
     * 
     * @param expected the test object
     */
    private void assertPofFidelity(final Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertEquals(expected, result);

    }
    /**
     * Check using EqualsBuilder.
     * @param expected the test object
     */
    private void assertPofFidelityByReflection(final Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertTrue(EqualsBuilder.reflectionEquals(expected, result));

    }


}
