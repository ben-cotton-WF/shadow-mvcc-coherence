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
import com.tangosol.util.extractor.PofExtractor;

public abstract class AbstractEntryWrapper implements EntryWrapper {

	protected BinaryEntry parentEntry;

	private boolean priorRead = false;
	protected TransactionId transactionId;
	protected String vcacheName;
	protected IsolationLevel isolationLevel;

	public AbstractEntryWrapper(BinaryEntry parentEntry, TransactionId transactionId, IsolationLevel isolationLevel, String vcacheName) {
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

	@SuppressWarnings("rawtypes")
	private BinaryEntry getPriorBinaryEntry() {
		BinaryEntry priorEntry = null;
		
		Binary priorBinaryKey = getPriorBinaryKey();
	
		if (priorBinaryKey != null) {
			priorEntry = (BinaryEntry) getBackingMapContext().getBackingMapEntry(priorBinaryKey);
	
			boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(priorEntry);
			if (isolationLevel != IsolationLevel.readUncommitted) {
				if (!committed) {
					throw new UncommittedReadException((VersionedKey)getContext().getKeyFromInternalConverter().convert(priorBinaryKey));
				}
			}
			
			boolean deleted = (Boolean) Constants.DELETESTATUSEXTRACTOR.extractFromEntry(priorEntry);
			if (deleted && committed) {
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
	public Binary getBinaryKey() {
		return parentEntry.getBinaryKey();
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

	public boolean isPriorRead() {
		return priorRead;
	}

}