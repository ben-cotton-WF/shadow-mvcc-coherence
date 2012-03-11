package com.sixwhits.cohmvcc.invocable;

import java.util.Set;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.EntryAggregator;

@Portable
public class AggregatorWrapper implements EntryAggregator {

	private static final long serialVersionUID = 7022940621318350536L;

	public static final int POF_DELEGATE = 0;
	@PortableProperty(POF_DELEGATE)
	private EntryAggregator delegate;
	
	public AggregatorWrapper() {
		super();
	}

	public AggregatorWrapper(EntryAggregator delegate) {
		super();
		this.delegate = delegate;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object aggregate(Set set) {
		return delegate.aggregate(new VersionWrapperSet(set));
	}

}
