package com.sixwhits.cohmvcc.transaction.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class ExistenceCheckProcessor extends AbstractProcessor {

	private static final long serialVersionUID = 6193489100316487489L;

	@Override
	public Object process(Entry entry) {
		return entry.isPresent();
	}

}
