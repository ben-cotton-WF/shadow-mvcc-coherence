package com.sixwhits.cohmvcc.index;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.domain.ExtractorWrapper;
import com.tangosol.util.ValueExtractor;

@SuppressWarnings("rawtypes")
public class IndexMapWrapper implements Map {

	private final Map delegate;
	
	
	public IndexMapWrapper(Map delegate) {
		super();
		this.delegate = delegate;
	}

	@Override
	public int size() {
		return delegate.size();
	}

	@Override
	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return delegate.containsKey(new ExtractorWrapper((ValueExtractor) key));
	}

	@Override
	public boolean containsValue(Object value) {
		return delegate.containsValue(value);
	}

	@Override
	public Object get(Object key) {
		return delegate.get(new ExtractorWrapper((ValueExtractor) key));
	}

	@Override
	public Object put(Object key, Object value) {
		throw new UnsupportedOperationException("read-only");
	}

	@Override
	public Object remove(Object key) {
		throw new UnsupportedOperationException("read-only");
	}

	@Override
	public void putAll(Map m) {
		throw new UnsupportedOperationException("read-only");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("read-only");
	}

	@Override
	public Set keySet() {
		throw new UnsupportedOperationException("that's too much like hard work, let's hope it doesn't get called");
	}

	@Override
	public Collection values() {
		return delegate.values();
	}

	@Override
	public Set entrySet() {
		throw new UnsupportedOperationException("that's too much like hard work, let's hope it doesn't get called");
	}

}
