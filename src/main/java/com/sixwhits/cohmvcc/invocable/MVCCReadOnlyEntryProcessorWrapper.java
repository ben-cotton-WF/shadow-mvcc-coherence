package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

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
			EntryProcessor delegate, IsolationLevel isolationLevel, CacheName cacheName) {
		super(transactionId, isolationLevel, cacheName);
		this.delegate = delegate;
	}

	public MVCCReadOnlyEntryProcessorWrapper(TransactionId transactionId,
			EntryProcessor delegate, IsolationLevel isolationLevel, CacheName cacheName, Filter filter) {
		super(transactionId, isolationLevel, cacheName, filter);
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
				return new ProcessorResult<K,R>(null, (VersionedKey<K>)priorEntry.getKey());
			}
		}

		boolean deleted = (Boolean) Constants.DELETESTATUSEXTRACTOR.extractFromEntry(priorEntry);
		if (deleted) {
			return null;
		}

		R result = null;
		
		if (delegate != null) {

			ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, transactionId, isolationLevel, cacheName);
			
			if (!confirmFilterMatch(childEntry)) {
				return null;
			}

			result = (R) delegate.process(childEntry);
		
		}
		
		if ((isolationLevel == IsolationLevel.repeatableRead || isolationLevel == IsolationLevel.serializable)) {
			setReadTimestamp(entry);
		}
		
		return new ProcessorResult<K, R>(result, null);
	}

}
