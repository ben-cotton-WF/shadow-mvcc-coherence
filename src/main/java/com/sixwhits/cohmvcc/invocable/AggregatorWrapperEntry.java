package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.Constants;
import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.PofExtractor;

public class AggregatorWrapperEntry implements BinaryEntry {

	private final BinaryEntry underlying;
	
	public AggregatorWrapperEntry(BinaryEntry underlying) {
		super();
		this.underlying = underlying;
	}

	@Override
	public Object getKey() {
		return getBackingMapContext().getManagerContext().getKeyFromInternalConverter().convert(getBinaryKey());
	}

	@Override
	public Object getValue() {
		return getBackingMapContext().getManagerContext().getValueFromInternalConverter().convert(getBinaryValue());
	}

	@Override
	public Object setValue(Object obj) {
		throw new UnsupportedOperationException("read only entry");
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

	@Override
	public Object extract(ValueExtractor valueextractor) {
		if (valueextractor instanceof PofExtractor) {
			return ((PofExtractor)valueextractor).extractFromEntry(this);
		} else {
			return valueextractor.extract(getValue());
		}
	}

	@Override
	public Binary getBinaryKey() {
		return (Binary) Constants.KEYEXTRACTOR.extractFromEntry(underlying);
	}

	@Override
	public Binary getBinaryValue() {
		return (Binary) Constants.VALUEEXTRACTOR.extractFromEntry(underlying);
	}

	@Override
	public Serializer getSerializer() {
		return underlying.getSerializer();
	}

	@Override
	public BackingMapManagerContext getContext() {
		return underlying.getContext();
	}

	@Override
	public void updateBinaryValue(Binary binary) {
		throw new UnsupportedOperationException("read only entry");
	}

	@Override
	public Object getOriginalValue() {
		return getValue();
	}

	@Override
	public Binary getOriginalBinaryValue() {
		return getBinaryValue();
	}

	@Override
	public ObservableMap getBackingMap() {
		throw new UnsupportedOperationException("fake entry");
	}

	@Override
	public BackingMapContext getBackingMapContext() {
		return underlying.getBackingMapContext();
	}

	@Override
	public void expire(long l) {
		throw new UnsupportedOperationException("read only entry");
	}

	@Override
	public boolean isReadOnly() {
		return underlying.isReadOnly();
	}

}
