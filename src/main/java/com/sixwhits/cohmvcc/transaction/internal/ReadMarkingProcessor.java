package com.sixwhits.cohmvcc.transaction.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.invocable.AbstractMVCCProcessor;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;

@Portable
public class ReadMarkingProcessor<K> extends AbstractMVCCProcessor<K> {

	private static final long serialVersionUID = -6559372127281694088L;

	public ReadMarkingProcessor() {
		super();
	}

	public ReadMarkingProcessor(TransactionId transactionId,
			IsolationLevel isolationLevel, String vcacheName) {
		super(transactionId, isolationLevel, vcacheName);
	}

	@Override
	public Object process(Entry arg) {
		BinaryEntry entry = (BinaryEntry) arg;
		Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
		if (priorVersionBinaryKey == null) {
			return null;
		}

		BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry).getBackingMapEntry(priorVersionBinaryKey);

		boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(priorEntry);
		if (!committed) {
			return priorEntry.getKey();
		}

		boolean deleted = (Boolean) Constants.DELETESTATUSEXTRACTOR.extractFromEntry(priorEntry);
		if (deleted) {
			return null;
		}

		setReadTimestamp(entry);
		
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map processAll(Set set) {
		Map<K,VersionedKey<K>> result = new HashMap<K, VersionedKey<K>>();
		
		for (Entry entry : (Set<Entry>) set) {
			VersionedKey<K> uncommittedVersion = (VersionedKey<K>) process(entry);
			if (uncommittedVersion != null) {
				result.put((K) entry.getKey(), uncommittedVersion);
			}
		}
		
		return result;
	}

}
