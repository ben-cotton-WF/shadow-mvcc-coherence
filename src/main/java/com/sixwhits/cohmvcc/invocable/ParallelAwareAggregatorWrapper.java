package com.sixwhits.cohmvcc.invocable;

import java.util.Collection;
import java.util.Set;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;

@Portable
public class ParallelAwareAggregatorWrapper implements ParallelAwareAggregator {

	private static final long serialVersionUID = -1892144268045361220L;
	
	public static final int POF_DELEGATE = 0;
	@PortableProperty(POF_DELEGATE)
	private ParallelAwareAggregator delegate;
	
	public ParallelAwareAggregatorWrapper() {
		super();
	}

	public ParallelAwareAggregatorWrapper(ParallelAwareAggregator delegate) {
		super();
		this.delegate = delegate;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object aggregate(Set set) {
		return delegate.aggregate(new VersionWrapperSet(set));
	}

	@Override
	public EntryAggregator getParallelAggregator() {
		return new AggregatorWrapper(delegate.getParallelAggregator());
	}

	@Override
	public Object aggregateResults(@SuppressWarnings("rawtypes") Collection collection) {
		return delegate.aggregateResults(collection);
	}

}
