package com.sixwhits.cohmvcc.event;

import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.PofExtractor;

public class SyntheticBinaryEntry<K,V> implements BinaryEntry {

	private final Binary binaryKey;
	private final Binary binaryValue;
	private final Serializer serializer;
	private final BackingMapContext backingMapContext;

	public SyntheticBinaryEntry(Binary binaryKey, Binary binaryValue,
			Serializer serializer, BackingMapContext backingMapContext) {
		super();
		this.binaryKey = binaryKey;
		this.binaryValue = binaryValue;
		this.serializer = serializer;
		this.backingMapContext = backingMapContext;
	}

	@Override
	public Object extract(ValueExtractor valueextractor) {
		if (valueextractor instanceof PofExtractor) {
			return ((PofExtractor) valueextractor).extractFromEntry(this);
		} else {
			return valueextractor.extract(getValue());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public K getKey() {
		return (K) ExternalizableHelper.fromBinary(binaryKey, getSerializer());
	}

	@Override
	public Binary getOriginalBinaryValue() {
		return null;
	}

	@Override
	public boolean isPresent() {
		return true;
	}

	@Override
	public Binary getBinaryKey() {
		return binaryKey;
	}

	@Override
	public Serializer getSerializer() {
		return serializer;
	}

	@Override
	public BackingMapManagerContext getContext() {
		return getBackingMapContext().getManagerContext();
	}

	@Override
	public Object getOriginalValue() {
		return null;
	}

	@Override
	public ObservableMap getBackingMap() {
		return getBackingMapContext().getBackingMap();
	}

	@Override
	public BackingMapContext getBackingMapContext() {
		return backingMapContext;
	}

	@Override
	public void expire(long l) {
		throw new UnsupportedOperationException("expiry of MVCC cache entries not supported");
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object getValue() {
		return (K) ExternalizableHelper.fromBinary(binaryValue, getSerializer());
	}

	@Override
	public Object setValue(Object obj) {
		throw new UnsupportedOperationException("read-only entry");
	}

	@Override
	public void setValue(Object obj, boolean flag) {
		throw new UnsupportedOperationException("read-only entry");
	}

	@Override
	public void update(ValueUpdater valueupdater, Object obj) {
		throw new UnsupportedOperationException("read-only entry");
	}

	@Override
	public void remove(boolean flag) {
		throw new UnsupportedOperationException("read-only entry");
	}

	@Override
	public Binary getBinaryValue() {
		return binaryValue;
	}

	@Override
	public void updateBinaryValue(Binary binary) {
		throw new UnsupportedOperationException("read-only entry");
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}
}