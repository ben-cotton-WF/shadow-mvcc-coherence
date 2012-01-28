package com.sixwhits.cohmvcc.index;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;

/**
 * @author David Whitmarsh from an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 */
@Portable
public class MVCCVersionFilter<K> implements IndexAwareFilter, Serializable {

	private static final long serialVersionUID = 5267677476884085089L;

	public static final int POF_TXID = 0;
	@PortableProperty(POF_TXID)
	private TransactionId transactionId;
	
	public static final int POF_KEY = 1;
	@PortableProperty(POF_KEY)
	private K nativeKey;
	
	
	public MVCCVersionFilter() {
		// required 
	}
	
	public MVCCVersionFilter(TransactionId transactionId, K key) {
		this.transactionId = transactionId;
		this.nativeKey = key;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean evaluateEntry(Entry paramEntry) {
		throw new UnsupportedOperationException("Cannot work without index");
	}

	@Override
	public boolean evaluate(Object paramObject) {
		throw new UnsupportedOperationException("Cannot work without index");
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int calculateEffectiveness(Map mapIndex, Set candidates) {
		// cannot work without index anyway
		return 1;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Filter applyIndex(Map indexes, Set candidates) {
		candidates.clear();
		MVCCIndex index = getIndex(indexes);
		Binary k =  index.floor(nativeKey, transactionId);
		if (k != null) {
			candidates.add(k);
		}
		
		return null;
	}

	@SuppressWarnings("rawtypes")
	private MVCCIndex<?> getIndex(Map indexes) {
		for (Object mi : indexes.values()) {
			if (mi instanceof MVCCIndex) {
				return (MVCCIndex<?>) mi;
			}
		}
		throw new IllegalArgumentException("No MVCCIndex defined");
	}
}
