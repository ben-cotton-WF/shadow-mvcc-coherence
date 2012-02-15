package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.exception.UncommittedReadException;
import com.sixwhits.cohmvcc.index.MVCCIndex;
import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.extractor.PofUpdater;

public class EntryWrapper implements BinaryEntry {
	
	private BinaryEntry parentEntry;
	private boolean delete = false;
	private Binary newValue;
	private boolean priorRead = false;
	private TransactionId transactionId;
	private String vcacheName;
	private IsolationLevel isolationLevel;
	
	public EntryWrapper(BinaryEntry parentEntry, TransactionId transactionId, IsolationLevel isolationLevel, String vcacheName) {
		super();
		this.parentEntry = parentEntry;
		this.transactionId = transactionId;
		this.isolationLevel = isolationLevel;
		this.vcacheName = vcacheName;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Binary getPriorBinaryKey() {
		priorRead = true;
		MVCCIndex index = (MVCCIndex) getVersionCacheBackingMapContext().getIndexMap().get(AbstractMVCCProcessor.indexId);
		return index.floor(parentEntry.getKey(), transactionId);
	}
	
	private BackingMapContext getVersionCacheBackingMapContext() {
		return parentEntry.getBackingMapContext().getManagerContext().getBackingMapContext(vcacheName);
	}
	
	@Override
	public Object extract(ValueExtractor valueextractor) {
		if (valueextractor instanceof PofExtractor) {
			return ((PofExtractor) valueextractor).extractFromEntry(this);
		} else {
			return valueextractor.extract(getValue());
		}
	}

	@Override
	public Object getKey() {
		return parentEntry.getKey();
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

	@SuppressWarnings("rawtypes")
	private BinaryEntry getPriorBinaryEntry() {
		BinaryEntry priorEntry = null;
		
		Binary priorBinaryKey = getPriorBinaryKey();

		if (priorBinaryKey != null) {
			priorEntry = (BinaryEntry) getBackingMapContext().getBackingMapEntry(priorBinaryKey);

			if (isolationLevel != IsolationLevel.readUncommitted) {
				boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(priorEntry);
				if (!committed) {
					throw new UncommittedReadException((VersionedKey)getContext().getKeyFromInternalConverter().convert(priorBinaryKey));
				}
			}
			
			boolean deleted = (Boolean) Constants.DELETESTATUSEXTRACTOR.extractFromEntry(priorEntry);
			if (deleted) {
				priorEntry = null;
			}
		}
		
		return priorEntry;
		
	}

	@Override
	public Binary getOriginalBinaryValue() {

		BinaryEntry priorEntry = getPriorBinaryEntry();
		Binary result = null;
		
		if (priorEntry != null) {
			result = extractBinaryLogicalValue(priorEntry);
		}
		
		return result;
	}
	
	
	@Override
	public boolean isPresent() {
		BinaryEntry priorEntry = getPriorBinaryEntry();
		return (priorEntry != null);
	}

	@Override
	public void remove(boolean isSynthetic) {
		delete = true;
	}

	@Override
	public Binary getBinaryKey() {
		return parentEntry.getBinaryKey();
	}

	@Override
	public Binary getBinaryValue() {
		return newValue == null ? getOriginalBinaryValue() : newValue;
	}

	@Override
	public Serializer getSerializer() {
		return parentEntry.getSerializer();
	}

	@Override
	public BackingMapManagerContext getContext() {
		return getBackingMapContext().getManagerContext();
	}

	@Override
	public void updateBinaryValue(Binary binary) {
		newValue = binary;
	}

	@Override
	public Object getOriginalValue() {
		return getContext().getValueFromInternalConverter().convert(getOriginalBinaryValue());
	}


	private Binary extractBinaryLogicalValue(BinaryEntry priorEntry) {
		return (Binary) Constants.VALUEEXTRACTOR.extractFromEntry(priorEntry);
	}

	@Override
	public ObservableMap getBackingMap() {
		return getBackingMapContext().getBackingMap();
	}

	@Override
	public BackingMapContext getBackingMapContext() {
		return getVersionCacheBackingMapContext();
	}

	@Override
	public void expire(long l) {
		throw new UnsupportedOperationException("expiry of MVCC cache entries not supported");
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	public boolean isRemove() {
		return delete;
	}

	public Binary getNewBinaryValue() {
		return newValue;
	}
	
	public boolean isPriorRead() {
		return priorRead;
	}
}