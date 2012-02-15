package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

@Portable
public class MVCCReadOnlyEntryProcessorWrapper<K> extends AbstractMVCCProcessor<K> {

	private static final long serialVersionUID = -7158130705920331999L;

	public static final int POF_EP = 10;
	@PortableProperty(POF_EP)
	private EntryProcessor delegate;

	public MVCCReadOnlyEntryProcessorWrapper() {
		super();
	}

	public MVCCReadOnlyEntryProcessorWrapper(TransactionId transactionId,
			EntryProcessor delegate, IsolationLevel isolationLevel, String vcacheName) {
		super(transactionId, isolationLevel, vcacheName);
		this.delegate = delegate;
	}

	@Override
	public Object process(Entry entryarg) {
		
		BinaryEntry entry = (BinaryEntry) entryarg;
		
		ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, transactionId, isolationLevel, vcacheName);
		
		Object result = delegate.process(childEntry);
		
		if ((isolationLevel == IsolationLevel.repeatableRead || isolationLevel == IsolationLevel.serializable) && childEntry.isPriorRead()) {
			setReadTimestamp(entry);
		}
		
		return result;
	}

}
