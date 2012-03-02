package com.sixwhits.cohmvcc.invocable;

import java.util.Collection;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.tangosol.io.Serializer;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.CacheFactory;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;

@Portable
public class ParallelAwareAggregatorWrapper implements ParallelAwareAggregator {

	private static final long serialVersionUID = -1892144268045361220L;
	
	public static final int POF_DELEGATE = 0;
	@PortableProperty(POF_DELEGATE)
	private ParallelAwareAggregator delegate;
	public static final int POF_NAME = 1;
	@PortableProperty(POF_NAME)
	private CacheName cacheName;
	
	public ParallelAwareAggregatorWrapper() {
		super();
	}

	public ParallelAwareAggregatorWrapper(ParallelAwareAggregator delegate, CacheName cacheName) {
		super();
		this.delegate = delegate;
		this.cacheName = cacheName;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object aggregate(Set set) {
		Serializer serializer = CacheFactory.getCache(cacheName.getKeyCacheName()).getCacheService().getSerializer();
		return delegate.aggregate(new VersionWrapperSet(serializer, set));
	}

	@Override
	public EntryAggregator getParallelAggregator() {
		return new ParallelAwareAggregatorWrapper((ParallelAwareAggregator)delegate.getParallelAggregator(), cacheName);
	}

	@Override
	public Object aggregateResults(@SuppressWarnings("rawtypes") Collection collection) {
		return delegate.aggregateResults(collection);
	}

}
