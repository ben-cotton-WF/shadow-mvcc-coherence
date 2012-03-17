package com.sixwhits.cohmvcc.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

public class DomainSerialisationTest {

    private ConfigurablePofContext pofContext;
    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;

    @Before
    public void setUp() throws Exception {
        pofContext = new ConfigurablePofContext("mvcc-pof-config-test.xml");
    }

    @Test
    public void testTransactionId() {
        TransactionId vo = new TransactionId(BASETIME, 123, 456);
        assertPofFidelity(vo);
    }

    @Test
    public void testVersionedKey() {
        VersionedKey<Integer> vo = new VersionedKey<Integer>(982, new TransactionId(BASETIME + 17, 124, 457));
        assertPofFidelity(vo);
    }

    @Test
    public void testSampleDomainObject() {
        SampleDomainObject vo = new SampleDomainObject(123, "456");
        assertPofFidelity(vo);
    }

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

    public void testProcessorResult1() {
        TransactionId ts = new TransactionId(BASETIME + 17, 124, 457);
        ProcessorResult<String, Integer> pr = new ProcessorResult<String, Integer>(null, new VersionedKey<String>("ABC", ts));
        assertPofFidelityByReflection(pr);
    }
    public void testProcessorResult2() {
        ProcessorResult<String, Integer> pr = new ProcessorResult<String, Integer>(99, null);
        assertPofFidelityByReflection(pr);
    }


    private void assertPofFidelity(Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertEquals(expected, result);

    }
    private void assertPofFidelityByReflection(Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertTrue(EqualsBuilder.reflectionEquals(expected, result));

    }


}
