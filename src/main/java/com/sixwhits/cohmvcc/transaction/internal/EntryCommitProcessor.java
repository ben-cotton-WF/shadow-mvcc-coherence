package com.sixwhits.cohmvcc.transaction.internal;

import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.extractor.PofUpdater;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class EntryCommitProcessor extends AbstractProcessor {

	private static final long serialVersionUID = 2004629159766780786L;
	private static final PofUpdater commitUpdater = new PofUpdater(TransactionalValue.POF_COMMITTED);
	
	@Override
	public Object process(Entry arg) {
		BinaryEntry entry = (BinaryEntry) arg;
		commitUpdater.updateEntry(entry, Boolean.TRUE);
		return null;
	}
}
