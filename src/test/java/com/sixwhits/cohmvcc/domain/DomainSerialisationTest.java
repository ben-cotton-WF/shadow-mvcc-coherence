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
	
	@Before
	public void setUp() throws Exception {
		pofContext = new ConfigurablePofContext("mvcc-pof-config-test.xml"); 
	}

	@Test
	public void testTransactionalValue() {
		
		TransactionalValue vo = new TransactionalValue(true, false, new Binary(new byte[] { 0x01, 0x02, 0x03, 0x04 }));
		assertPofFidelity(vo);
	}
	
	@Test
	public void TestTransactionId() {
		TransactionId vo = new TransactionId(40L*365L*24L*60L*60L*1000L, 123, 456);
		assertPofFidelity(vo);
	}
	
	@Test
	public void TestVersionedKey() {
		VersionedKey<Integer> vo = new VersionedKey<Integer>(982, new TransactionId(40L*365L*24L*60L*60L*1000L + 17, 124, 457));
		assertPofFidelity(vo);
	}
	@Test
	public void TestSampleDomainObject() {
		
		SampleDomainObject vo = new SampleDomainObject(123, "456");
		assertPofFidelity(vo);
	}
	
	@Test
	public void testTransactionSetWrapper() {
		TransactionSetWrapper tsw = new TransactionSetWrapper();
		NavigableSet<TransactionId> ts = new TreeSet<TransactionId>();
		ts.add(new TransactionId(40L*365L*24L*60L*60L*1000L + 17, 124, 457));
		ts.add(new TransactionId(40L*365L*24L*60L*60L*1000L + 42, 124, 457));
		ts.add(new TransactionId(40L*365L*24L*60L*60L*1000L + 42, 124, 455));
		tsw.setTransactionIdSet(ts);
		assertPofFidelityByReflection(tsw);
	}
	
	public void testProcessorResult1() {
		TransactionId ts = new TransactionId(40L*365L*24L*60L*60L*1000L + 17, 124, 457);
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
