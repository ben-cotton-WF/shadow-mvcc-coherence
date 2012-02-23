package com.sixwhits.cohmvcc.cache.internal;

import java.util.Map;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class PutAllProcessor<K,V> extends AbstractProcessor {

	private static final long serialVersionUID = -5621228179782770648L;

	public static final int POF_VALUEMAP = 0;
	@PortableProperty(POF_VALUEMAP)
	private Map<K, V> valueMap;
	
	public PutAllProcessor() {
		super();
	}

	public PutAllProcessor(Map<K, V> valueMap) {
		super();
		this.valueMap = valueMap;
	}

	@Override
	public Object process(Entry entry) {
		entry.setValue(valueMap.get(entry.getKey()), false);
		return null;
	}

	public Map<K, V> getValueMap() {
		return valueMap;
	}

}
