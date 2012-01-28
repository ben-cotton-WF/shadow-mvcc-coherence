package com.sixwhits.cohmvcc.domain;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

public class DomainSerialisationTest {

	private ConfigurablePofContext pofContext;
	
	@Before
	public void setUp() throws Exception {
		pofContext = new ConfigurablePofContext("mvcc-pof-config.xml"); 
	}

	@Test
	public void testTransactionalValue() {
		
		TransactionalValue<Integer> vo = new TransactionalValue<Integer>(TransactionStatus.rolledback, 99);
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
	
	private void assertPofFidelity(Object expected) {
		Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
		Object result = ExternalizableHelper.fromBinary(binary, pofContext);
		
		assertEquals(expected, result);
		
	}

}
