package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.repeatableRead;
import static com.sixwhits.cohmvcc.domain.IsolationLevel.serializable;

import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.ProcessorResult;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.invocable.AbstractMVCCProcessor;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;

@Portable
public class ReadMarkingProcessor<K> extends AbstractMVCCProcessor<K,Object> {

	private static final long serialVersionUID = -6559372127281694088L;

	public ReadMarkingProcessor() {
		super();
	}

	public ReadMarkingProcessor(TransactionId transactionId,
			IsolationLevel isolationLevel, String vcacheName) {
		super(transactionId, isolationLevel, vcacheName);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ProcessorResult<K,Object> process(Entry arg) {
		BinaryEntry entry = (BinaryEntry) arg;
		Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
		if (priorVersionBinaryKey == null) {
			return null;
		}

		BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry).getBackingMapEntry(priorVersionBinaryKey);

		if (isolationLevel != readCommitted) {
			boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(priorEntry);
			if (!committed) {
				return new ProcessorResult<K,Object>((VersionedKey<K>)priorEntry.getKey());
			}
		}

		boolean deleted = (Boolean) Constants.DELETESTATUSEXTRACTOR.extractFromEntry(priorEntry);
		if (deleted) {
			return null;
		}

		if (isolationLevel == repeatableRead || isolationLevel == serializable) {
			setReadTimestamp(entry);
		}
		
		return null;
	}

	public IsolationLevel getIsolationLevel() {
		return isolationLevel;
	}

}
