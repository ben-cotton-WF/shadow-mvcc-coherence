package com.sixwhits.cohmvcc.index;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

public class IndexSerialisationTest {

	private ConfigurablePofContext pofContext;
	
	@Before
	public void setUp() throws Exception {
		pofContext = new ConfigurablePofContext("mvcc-pof-config.xml"); 
	}

	@Test
	public void testMVCCSurfaceFilter() {
		
		MVCCSurfaceFilter vo = new MVCCSurfaceFilter(new TransactionId(40L*365L*24L*60L*60L*1000L + 17, 124, 457));
		assertPofFidelity(vo);
	}
	
	private void assertPofFidelity(Object expected) {
		Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
		Object result = ExternalizableHelper.fromBinary(binary, pofContext);
		
		assertEquals(expected, result);
	}

}
