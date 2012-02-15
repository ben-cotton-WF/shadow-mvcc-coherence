package com.sixwhits.cohmvcc.transaction.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class EntryRollbackProcessor extends AbstractProcessor {

	private static final long serialVersionUID = 3573370467378537711L;

	@Override
	public Object process(Entry entry) {
		entry.remove(false);
		return null;
	}
}
