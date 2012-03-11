package com.sixwhits.cohmvcc.transaction.internal;

import com.sixwhits.cohmvcc.domain.Utils;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class EntryCommitProcessor extends AbstractProcessor {

	private static final long serialVersionUID = 2004629159766780786L;
	
	@Override
	public Object process(Entry arg) {
		BinaryEntry entry = (BinaryEntry) arg;
		Utils.setCommitted(entry, true);
		return null;
	}
}
