package com.sixwhits.cohmvcc.processor;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.processor.ConditionalPut;

public class ProcessorSerialisationTest {

	private ConfigurablePofContext pofContext;
	
	@Before
	public void setUp() throws Exception {
		pofContext = new ConfigurablePofContext("mvcc-pof-config.xml"); 
	}

	@Test
	public void testMVCCSurfaceFilter() {
		
		MVCCEntryProcessorWrapper<String> wrapper = new MVCCEntryProcessorWrapper<String>(
				new TransactionId(40L*365L*24L*60L*60L*1000L + 17, 124, 457),
				new ConditionalPut(AlwaysFilter.INSTANCE, "a test value"),
				IsolationLevel.serializable, false, "acachename");
		
		assertPofFidelity(wrapper);	
	}

	
	private void assertPofFidelity(Object expected) {
		Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
		Object result = ExternalizableHelper.fromBinary(binary, pofContext);
		
		assertTrue(EqualsBuilder.reflectionEquals(expected, result));
	}

}
