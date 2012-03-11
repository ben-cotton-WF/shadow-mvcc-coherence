package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.PofUpdater;

public class ReadWriteEntryWrapper extends AbstractEntryWrapper implements EntryWrapper {
	
	private boolean delete = false;
	private Binary newBinaryValue;
	
	public ReadWriteEntryWrapper(BinaryEntry parentEntry, TransactionId transactionId, IsolationLevel isolationLevel, CacheName cacheName) {
		super(parentEntry, transactionId, isolationLevel, cacheName);
	}

	@Override
	public Object getValue() {
		Binary binaryValue = getBinaryValue();
		return binaryValue == null ? null : getContext().getValueFromInternalConverter().convert(binaryValue);
	}

	@Override
	public Object setValue(Object obj) {
		Object result = getValue();
		newBinaryValue = (Binary) getBackingMapContext().getManagerContext().getValueToInternalConverter().convert(obj);
		return result;
	}

	@Override
	public void setValue(Object obj, boolean flag) {
		newBinaryValue = (Binary) getBackingMapContext().getManagerContext().getValueToInternalConverter().convert(obj);
	}

	@Override
	public void update(ValueUpdater valueupdater, Object obj) {
		if (valueupdater instanceof PofUpdater) {
			((PofUpdater) valueupdater).updateEntry(this, obj);
		} else {
			valueupdater.update(getValue(), obj);
		}
	}

	@Override
	public void remove(boolean isSynthetic) {
		delete = true;
	}

	@Override
	public Binary getBinaryValue() {
		return newBinaryValue == null ? getOriginalBinaryValue() : newBinaryValue;
	}

	@Override
	public void updateBinaryValue(Binary binary) {
		newBinaryValue = binary;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	/* (non-Javadoc)
	 * @see com.sixwhits.cohmvcc.invocable.EntryWrapper#isRemove()
	 */
	@Override
	public boolean isRemove() {
		return delete;
	}

	public Binary getNewBinaryValue() {
		return newBinaryValue;
	}
}