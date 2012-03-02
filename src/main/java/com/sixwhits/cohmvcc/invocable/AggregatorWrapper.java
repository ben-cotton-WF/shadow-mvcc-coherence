package com.sixwhits.cohmvcc.invocable;

import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.tangosol.io.Serializer;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.CacheFactory;
import com.tangosol.util.InvocableMap.EntryAggregator;

@Portable
public class AggregatorWrapper implements EntryAggregator {

	private static final long serialVersionUID = 7022940621318350536L;

	public static final int POF_DELEGATE = 0;
	@PortableProperty(POF_DELEGATE)
	private EntryAggregator delegate;
	public static final int POF_NAME = 1;
	@PortableProperty(POF_NAME)
	private CacheName cacheName;
	
	public AggregatorWrapper() {
		super();
	}

	public AggregatorWrapper(EntryAggregator delegate, CacheName cacheName) {
		super();
		this.delegate = delegate;
		this.cacheName = cacheName;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object aggregate(Set set) {
		Serializer serializer = CacheFactory.getCache(cacheName.getKeyCacheName()).getCacheService().getSerializer();
		return delegate.aggregate(new VersionWrapperSet(serializer, set));
	}

}
