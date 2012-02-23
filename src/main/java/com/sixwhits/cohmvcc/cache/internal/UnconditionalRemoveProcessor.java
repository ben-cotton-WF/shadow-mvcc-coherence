package com.sixwhits.cohmvcc.cache.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class UnconditionalRemoveProcessor extends AbstractProcessor {

	private static final long serialVersionUID = -1589869423220481276L;
	public static final int POF_RETURN = 0;
	@PortableProperty(POF_RETURN)
	private boolean returnPrior = true;

	public UnconditionalRemoveProcessor() {
		super();
	}

	public UnconditionalRemoveProcessor(boolean returnPrior) {
		super();
		this.returnPrior = returnPrior;
	}

	@Override
	public Object process(Entry entry) {
		entry.remove(false);
		return returnPrior ? entry.getValue() : null;
	}

	public boolean isReturnPrior() {
		return returnPrior;
	}

}
