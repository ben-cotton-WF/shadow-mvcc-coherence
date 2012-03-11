package com.sixwhits.cohmvcc.cache.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class UnconditionalPutProcessor extends AbstractProcessor {

	private static final long serialVersionUID = 1660410324368769032L;
	public static final int POF_RETURNPRIOR = 0;
	@PortableProperty(POF_RETURNPRIOR)
	private boolean returnPrior;
	public static final int POF_VALUE = 1;
	@PortableProperty(POF_VALUE)
	private Object value;
	
	public UnconditionalPutProcessor() {
		super();
	}

	public UnconditionalPutProcessor(Object value, boolean returnPrior) {
		super();
		this.returnPrior = returnPrior;
		this.value = value;
	}

	@Override
	public Object process(Entry entry) {
		// Need to replace this to not deserialise value
		Object result = returnPrior ? entry.getValue() : null;
		entry.setValue(value, false);
		return result;
	}

	public boolean isReturnPrior() {
		return returnPrior;
	}

	public Object getValue() {
		return value;
	}

}
