package com.sixwhits.cohmvcc.processor;

import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

public class CacheGetProcessor extends AbstractProcessor {

	private Long readTimestamp;
	
	public Object process(Entry entry) {
		
		if (!entry.isPresent()) {
			return null;
		}
		
		
		return null;
	}

}
