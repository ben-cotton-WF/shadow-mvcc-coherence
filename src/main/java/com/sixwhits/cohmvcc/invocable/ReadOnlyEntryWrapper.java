package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ValueUpdater;

public class ReadOnlyEntryWrapper extends AbstractEntryWrapper {


	public ReadOnlyEntryWrapper(BinaryEntry parentEntry,
			TransactionId transactionId, IsolationLevel isolationLevel,
			CacheName cacheName) {
		super(parentEntry, transactionId, isolationLevel, cacheName);
	}

	@Override
	public Binary getBinaryValue() {
		return getOriginalBinaryValue();
	}

	@Override
	public void updateBinaryValue(Binary binary) {
		throw new UnsupportedOperationException("Read only entry");
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public Object getValue() {
		return getOriginalValue();
	}

	@Override
	public Object setValue(Object obj) {
		throw new UnsupportedOperationException("Read only entry");
	}

	@Override
	public void setValue(Object obj, boolean flag) {
		throw new UnsupportedOperationException("Read only entry");
	}

	@Override
	public void update(ValueUpdater valueupdater, Object obj) {
		throw new UnsupportedOperationException("Read only entry");
	}

	@Override
	public void remove(boolean flag) {
		throw new UnsupportedOperationException("Read only entry");
	}

	@Override
	public boolean isRemove() {
		return false;
	}

}
