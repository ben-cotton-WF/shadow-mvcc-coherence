package com.sixwhits.cohmvcc.invocable;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.util.BinaryEntry;

public class ReadWriteEntryWrapperTest {

	private static final long BASETIME = 40L*365L*24L*60L*60L*1000L;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Ignore
	@Test
	public void testGetValuePutValue() {
		
		BinaryEntry mockParent = mock(BinaryEntry.class);
		TransactionId ts = new TransactionId(BASETIME, 0, 0);
		
		ReadWriteEntryWrapper testWrapper = new ReadWriteEntryWrapper(mockParent, ts, readCommitted, new CacheName("testcache"));
		
		
		
	}

}
