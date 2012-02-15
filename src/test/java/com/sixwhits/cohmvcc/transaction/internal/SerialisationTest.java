package com.sixwhits.cohmvcc.transaction.internal;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.cache.internal.UnconditionalPutProcessor;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.invocable.MVCCEntryProcessorWrapper;
import com.sixwhits.cohmvcc.transaction.internal.ReadMarkingProcessor;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.processor.ConditionalPut;

public class SerialisationTest {

	private ConfigurablePofContext pofContext;
	
	@Before
	public void setUp() throws Exception {
		pofContext = new ConfigurablePofContext("mvcc-pof-config.xml"); 
	}

	@Test
	public void testReadMarkingProcessor() {
		ReadMarkingProcessor<Integer> obj = new ReadMarkingProcessor<Integer>(
				new TransactionId(40L*365L*24L*60L*60L*1000L + 17, 124, 457),
				IsolationLevel.serializable, "acachename");
		
		assertPofFidelity(obj);
	}
	
	@Test
	public void testEntryCommitProcessor() {
		Object obj = new EntryCommitProcessor();
		assertPofFidelity(obj);
	}

	@Test
	public void testEntryRollbackProcessor() {
		Object obj = new EntryRollbackProcessor();
		assertPofFidelity(obj);
	}
	

	private void assertPofFidelity(Object expected) {
		Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
		Object result = ExternalizableHelper.fromBinary(binary, pofContext);
		
		assertTrue(EqualsBuilder.reflectionEquals(expected, result));
	}

}
