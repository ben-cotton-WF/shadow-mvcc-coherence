package com.sixwhits.cohmvcc.invocable;

import java.util.NavigableSet;
import java.util.TreeSet;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionSetWrapper;
import com.sixwhits.cohmvcc.index.MVCCExtractor;
import com.sixwhits.cohmvcc.index.MVCCIndex;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public abstract class AbstractMVCCProcessor<K> extends AbstractProcessor {

	private static final long serialVersionUID = -8977457529050193716L;
	public static final int POF_TID = 1;
	@PortableProperty(POF_TID)
	protected TransactionId transactionId;
	public static final int POF_ISOLATION = 2;
	@PortableProperty(POF_ISOLATION)
	protected IsolationLevel isolationLevel;
	public static final int POF_VCACHENAME = 3;
	@PortableProperty(POF_VCACHENAME)
	protected String vcacheName;
	protected static final MVCCExtractor indexId = new MVCCExtractor();
	
	public AbstractMVCCProcessor(TransactionId transactionId,
			IsolationLevel isolationLevel, String vcacheName) {
		super();
		this.transactionId = transactionId;
		this.isolationLevel = isolationLevel;
		this.vcacheName = vcacheName;
	}

	public AbstractMVCCProcessor() {
		super();
	}

	public abstract Object process(Entry entryarg);

	protected NavigableSet<TransactionId> getReadTransactions(Entry entry) {
		TransactionSetWrapper tsw = (TransactionSetWrapper)entry.getValue();
		return tsw == null ? null : tsw.getTransactionIdSet();
	}

	protected void setReadTransactions(Entry entry, NavigableSet<TransactionId> readTimestamps) {
		TransactionSetWrapper tsw = new TransactionSetWrapper();
		tsw.setTransactionIdSet(readTimestamps);
		entry.setValue(tsw);
	}

	@SuppressWarnings("unchecked")
	protected TransactionId getNextWrite(BinaryEntry entry) {
		MVCCIndex<K> index = (MVCCIndex<K>) entry.getBackingMapContext().getIndexMap().get(indexId);
		return index.ceilingTid((K)entry.getKey(), transactionId);
	}

	protected TransactionId getNextRead(Entry entry) {
		NavigableSet<TransactionId> readTimestamps = getReadTransactions(entry);
		if (readTimestamps == null) {
			return null;
		}
		return readTimestamps.ceiling(transactionId);
	}
	
	protected BackingMapContext getVersionCacheBackingMapContext(BinaryEntry parentEntry) {
		return parentEntry.getBackingMapContext().getManagerContext().getBackingMapContext(vcacheName);
	}
	
	@SuppressWarnings("unchecked")
	protected Binary getPriorVersionBinaryKey(BinaryEntry parentEntry) {
		
		MVCCIndex<K> index = (MVCCIndex<K>) getVersionCacheBackingMapContext(parentEntry).getIndexMap().get(indexId);
		return index.floor((K) parentEntry.getKey(), transactionId);
		
	}
	
	protected void setReadTimestamp(BinaryEntry entry) {
		NavigableSet<TransactionId> readTimestamps = getReadTransactions(entry);
		if (readTimestamps == null) {
			readTimestamps = new TreeSet<TransactionId>();
		}
		readTimestamps.add(transactionId);
		setReadTransactions(entry, readTimestamps);
	}

}