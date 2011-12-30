package com.sixwhits.cohmvcc.processor;

import com.sixwhits.cohmvcc.domain.KeyTimestamps;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

public class CacheGetProcessor extends AbstractProcessor {

	private Long readTimestamp;
	
	public Object process(Entry entry) {
		
		if (!entry.isPresent()) {
			return null;
		}
		
		KeyTimestamps ts = (KeyTimestamps) entry.getValue();
		Long latestVersion = ts.getPrecedingWriteTimestamp(readTimestamp);
		if (latestVersion == null) {
			return null;
		}
		
		
		return null;
	}

}
