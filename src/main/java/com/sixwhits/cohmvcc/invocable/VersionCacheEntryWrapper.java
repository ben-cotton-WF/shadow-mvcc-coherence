package com.sixwhits.cohmvcc.invocable;


import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.Serializer;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;

public class VersionCacheEntryWrapper<K, V> implements Entry {

	private final Entry underlying;
	private final Serializer serializer;
	
	public VersionCacheEntryWrapper(Serializer serializer, Entry underlying) {
		super();
		this.serializer = serializer;
		this.underlying = underlying;
	}

	@SuppressWarnings("unchecked")
	@Override
	public K getKey() {
		return ((VersionedKey<K>)underlying.getKey()).getNativeKey();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V getValue() {
		return (V) ExternalizableHelper.fromBinary(((TransactionalValue)underlying.getValue()).getValue(), serializer);
	}

	@Override
	public V setValue(Object obj) {
		throw new UnsupportedOperationException("read only entry");
	}

	@Override
	public Object extract(ValueExtractor valueextractor) {
		return valueextractor.extract(getValue());
	}

	@Override
	public void setValue(Object obj, boolean flag) {
		throw new UnsupportedOperationException("read only entry");
	}

	@Override
	public void update(ValueUpdater valueupdater, Object obj) {
		throw new UnsupportedOperationException("read only entry");
	}

	@Override
	public boolean isPresent() {
		return underlying.isPresent();
	}

	@Override
	public void remove(boolean flag) {
		throw new UnsupportedOperationException("read only entry");
	}

}
