package com.sixwhits.cohmvcc.cache.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class UnconditionalRemoveProcessor extends AbstractProcessor {

	private static final long serialVersionUID = -1589869423220481276L;

	@Override
	public Object process(Entry entry) {
		entry.remove(false);
		return entry.getValue();
	}

}
