package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

// TODO is this redundant?
@Portable
public class MVCCReadOnlyEntryProcessorWrapper<K,R> extends AbstractMVCCProcessor<K,R> {

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

	@SuppressWarnings("unchecked")
	@Override
	public ProcessorResult<K,R> process(Entry entryarg) {
		
		BinaryEntry entry = (BinaryEntry) entryarg;
		Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
		if (priorVersionBinaryKey == null) {
			return null;
		}

		BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry).getBackingMapEntry(priorVersionBinaryKey);

		if (isolationLevel != IsolationLevel.readUncommitted) {
			boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(priorEntry);
			if (!committed) {
				return new ProcessorResult<K,R>((VersionedKey<K>)priorEntry.getKey());
			}
		}

		boolean deleted = (Boolean) Constants.DELETESTATUSEXTRACTOR.extractFromEntry(priorEntry);
		if (deleted) {
			return null;
		}

		Object result = null;
		
		if (delegate != null) {

			ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, transactionId, isolationLevel, vcacheName);

			result = delegate.process(childEntry);
		
		}
		
		if ((isolationLevel == IsolationLevel.repeatableRead || isolationLevel == IsolationLevel.serializable)) {
			setReadTimestamp(entry);
		}
		
		return result == null ? null : new ProcessorResult<K, R>((R)result);
	}

}
