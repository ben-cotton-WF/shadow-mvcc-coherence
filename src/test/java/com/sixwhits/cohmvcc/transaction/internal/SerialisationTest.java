package com.sixwhits.cohmvcc.transaction.internal;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

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
				IsolationLevel.serializable, new CacheName("acachename"));
		
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
	@Test
	public void testExistenceCheckProcessor() {
		Object obj = new ExistenceCheckProcessor();
		assertPofFidelity(obj);
	}
	

	private void assertPofFidelity(Object expected) {
		Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
		Object result = ExternalizableHelper.fromBinary(binary, pofContext);
		
		assertTrue(EqualsBuilder.reflectionEquals(expected, result));
	}

}
