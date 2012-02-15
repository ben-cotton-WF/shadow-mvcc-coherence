package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.exception.FutureReadException;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

@Portable
public class MVCCEntryProcessorWrapper<K> extends AbstractMVCCProcessor<K> {

	private static final long serialVersionUID = -7158130705920331999L;


	public static final int POF_EP = 10;
	@PortableProperty(POF_EP)
	private EntryProcessor delegate;
	public static final int POF_AUTOCOMMIT = 11;
	@PortableProperty(POF_AUTOCOMMIT)
	protected boolean autoCommit = false;

	public MVCCEntryProcessorWrapper() {
	}

	
	public MVCCEntryProcessorWrapper(TransactionId transactionId,
			EntryProcessor delegate, IsolationLevel isolationLevel, boolean autoCommit, String vcacheName) {
		super(transactionId, isolationLevel, vcacheName);
		this.delegate = delegate;
		this.autoCommit = autoCommit;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object process(Entry entryarg) {
		
		BinaryEntry entry = (BinaryEntry) entryarg;
		
		ReadWriteEntryWrapper childEntry = new ReadWriteEntryWrapper(entry, transactionId, isolationLevel, vcacheName);
		
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
			TransactionalValue value = new TransactionalValue(autoCommit, childEntry.isRemove(),
					childEntry.isRemove() ? childEntry.getOriginalBinaryValue() : childEntry.getNewBinaryValue());
			Binary binaryValue = (Binary) childEntry.getContext().getValueToInternalConverter().convert(value);
			newEntry.updateBinaryValue(binaryValue);
		}
		
		if ((isolationLevel == IsolationLevel.repeatableRead || isolationLevel == IsolationLevel.serializable) && childEntry.isPriorRead()) {
			setReadTimestamp(entry);
		}
		
		return result;
	}

}
