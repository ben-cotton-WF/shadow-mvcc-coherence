package com.sixwhits.cohmvcc.processor;

import java.util.NavigableSet;
import java.util.TreeSet;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionSetWrapper;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.exception.FutureReadException;
import com.sixwhits.cohmvcc.exception.UncommittedReadException;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCIndex;
import com.tangosol.io.Serializer;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.extractor.PofUpdater;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class MVCCEntryProcessorWrapper<K> extends AbstractProcessor {

	private static final long serialVersionUID = -7158130705920331999L;

	public class EntryWrapper implements BinaryEntry {
		
		private BinaryEntry parentEntry;
		private boolean delete = false;
		private Binary newValue;
		private boolean priorRead = false;
		
		public EntryWrapper(BinaryEntry parentEntry) {
			super();
			this.parentEntry = parentEntry;
		}

		@SuppressWarnings("unchecked")
		private Binary getPriorBinaryKey() {
			
			priorRead = true;
			
			MVCCIndex<K> index = (MVCCIndex<K>) getBackingMapContext().getIndexMap().get(indexId);
			return index.floor((K) parentEntry.getKey(), transactionId);
			
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

		@SuppressWarnings("unchecked")
		private BinaryEntry getPriorBinaryEntry() {
			BinaryEntry priorEntry = null;
			
			Binary priorBinaryKey = getPriorBinaryKey();

			if (priorBinaryKey != null) {
				priorEntry = (BinaryEntry) getBackingMapContext().getBackingMapEntry(priorBinaryKey);

				if (isolationLevel != IsolationLevel.readUncommitted) {
					boolean committed = (Boolean) commitStatusExtractor.extractFromEntry(priorEntry);
					if (!committed) {
						throw new UncommittedReadException((VersionedKey<K>)getContext().getKeyFromInternalConverter().convert(priorBinaryKey));
					}
				}
				
				boolean deleted = (Boolean) deleteStatusExtractor.extractFromEntry(priorEntry);
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
			return (Binary) valueExtractor.extractFromEntry(priorEntry);
		}

		@Override
		public ObservableMap getBackingMap() {
			return getBackingMapContext().getBackingMap();
		}

		@Override
		public BackingMapContext getBackingMapContext() {
			return parentEntry.getBackingMapContext().getManagerContext().getBackingMapContext(vcacheName);
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

	public static final int POF_TID = 1;
	@PortableProperty(POF_TID)
	private TransactionId transactionId;
	public static final int POF_EP = 2;
	@PortableProperty(POF_EP)
	private EntryProcessor delegate;
	public static final int POF_ISOLATION = 3;
	@PortableProperty(POF_ISOLATION)
	private IsolationLevel isolationLevel;
	public static final int POF_VCACHENAME = 4;
	@PortableProperty(POF_VCACHENAME)
	private String vcacheName;
	public static final int POF_AUTOCOMMIT = 5;
	@PortableProperty(POF_AUTOCOMMIT)
	private boolean autoCommit = false;
	
	private static final MVCCExtractor indexId = new MVCCExtractor();
	private static final PofExtractor valueExtractor = new PofExtractor(null, new SimplePofPath(TransactionalValue.POF_VALUE), AbstractExtractor.VALUE);
	private static final PofExtractor commitStatusExtractor = new PofExtractor(null, new SimplePofPath(TransactionalValue.POF_COMMITTED), AbstractExtractor.VALUE);
	private static final PofExtractor deleteStatusExtractor = new PofExtractor(null, new SimplePofPath(TransactionalValue.POF_DELETED), AbstractExtractor.VALUE);

	public MVCCEntryProcessorWrapper() {
	}

	
	public MVCCEntryProcessorWrapper(TransactionId transactionId,
			EntryProcessor delegate, IsolationLevel isolationLevel, boolean autoCommit, String vcacheName) {
		super();
		this.transactionId = transactionId;
		this.delegate = delegate;
		this.isolationLevel = isolationLevel;
		this.autoCommit = autoCommit;
		this.vcacheName = vcacheName;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object process(Entry entryarg) {
		
		BinaryEntry entry = (BinaryEntry) entryarg;
		
		EntryWrapper childEntry = new EntryWrapper(entry);
		
		Object result = delegate.process(childEntry);
		
		if (childEntry.isRemove() || childEntry.getNewBinaryValue() != null) {
			
			TransactionId nextRead = getNextRead(entry);
			if (nextRead != null) {
				TransactionId nextWrite = getNextWrite(childEntry);
				if (nextWrite == null || nextRead.compareTo(nextWrite) <= 0) {
					throw new FutureReadException(new VersionedKey<K>((K) childEntry.getKey(), nextRead));
				}
			}
			
			Binary binaryKey = (Binary) childEntry.getContext().getKeyToInternalConverter().convert(new VersionedKey<K>((K) childEntry.getKey(), transactionId));
			BinaryEntry newEntry = (BinaryEntry) childEntry.getBackingMapContext().getBackingMapEntry(binaryKey);
			TransactionalValue value = new TransactionalValue(autoCommit, childEntry.isRemove(), childEntry.getNewBinaryValue());
			Binary binaryValue = (Binary) childEntry.getContext().getValueToInternalConverter().convert(value);
			newEntry.updateBinaryValue(binaryValue);
		}
		
		if ((isolationLevel == IsolationLevel.repeatableRead || isolationLevel == IsolationLevel.serializable) && childEntry.isPriorRead()) {
			NavigableSet<TransactionId> readTimestamps = getReadTransactions(entry);
			if (readTimestamps == null) {
				readTimestamps = new TreeSet<TransactionId>();
			}
			readTimestamps.add(transactionId);
			setReadTransactions(entry, readTimestamps);
		}
		
		return result;
	}
		

	private NavigableSet<TransactionId> getReadTransactions(Entry entry) {
		TransactionSetWrapper tsw = (TransactionSetWrapper)entry.getValue();
		return tsw == null ? null : tsw.getTransactionIdSet();
	}
	
	private void setReadTransactions(Entry entry, NavigableSet<TransactionId> readTimestamps) {
		TransactionSetWrapper tsw = new TransactionSetWrapper();
		tsw.setTransactionIdSet(readTimestamps);
		entry.setValue(tsw);
	}


	@SuppressWarnings("unchecked")
	private TransactionId getNextWrite(BinaryEntry entry) {
		MVCCIndex<K> index = (MVCCIndex<K>) entry.getBackingMapContext().getIndexMap().get(indexId);
		return index.ceilingTid((K)entry.getKey(), transactionId);
	}


	private TransactionId getNextRead(Entry entry) {
		NavigableSet<TransactionId> readTimestamps = getReadTransactions(entry);
		if (readTimestamps == null) {
			return null;
		}
		return readTimestamps.ceiling(transactionId);
	}

}
