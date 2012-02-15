package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.PofUpdater;

public class ReadWriteEntryWrapper extends AbstractEntryWrapper implements EntryWrapper {
	
	private boolean delete = false;
	private Binary newValue;
	
	public ReadWriteEntryWrapper(BinaryEntry parentEntry, TransactionId transactionId, IsolationLevel isolationLevel, String vcacheName) {
		super(parentEntry, transactionId, isolationLevel, vcacheName);
	}

	@Override
	public Object getValue() {
		Binary binaryValue = getBinaryValue();
		return binaryValue == null ? null : getContext().getValueFromInternalConverter().convert(binaryValue);
	}

	@Override
	public Object setValue(Object obj) {
		Object result = getValue();
		newValue = (Binary) getBackingMapContext().getManagerContext().getValueToInternalConverter().convert(obj);
		return result;
	}

	@Override
	public void setValue(Object obj, boolean flag) {
		newValue = (Binary) getBackingMapContext().getManagerContext().getValueToInternalConverter().convert(obj);
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
		return newValue == null ? getOriginalBinaryValue() : newValue;
	}

	@Override
	public void updateBinaryValue(Binary binary) {
		newValue = binary;
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
		return newValue;
	}
}