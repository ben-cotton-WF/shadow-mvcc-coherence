package com.sixwhits.cohmvcc.index;

import java.io.Serializable;
import java.util.Collections;
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
 * @author David Whitmarsh
 */
@Portable
public class MVCCLowerFilter<K> implements IndexAwareFilter, Serializable {

	private static final long serialVersionUID = 5267677476884085089L;

	public static final int POF_TXID = 0;
	@PortableProperty(POF_TXID)
	private TransactionId transactionId;
	public static final int POF_KEY = 1;
	@PortableProperty(POF_KEY)
	private K key;
	
	public MVCCLowerFilter() {
		// required 
	}
	
	public MVCCLowerFilter(TransactionId transactionId, K key) {
		super();
		this.transactionId = transactionId;
		this.key = key;
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
		MVCCIndex<K> index = getIndex(indexes);
		Binary keyLower = index.lower(key, transactionId);
		if (keyLower != null) {
			candidates.retainAll(Collections.singleton(keyLower));
		} else {
			candidates.clear();
		}
		
		// no further filtering required
		return null;
	}

	@SuppressWarnings("rawtypes")
	private MVCCIndex<K> getIndex(Map indexes) {
		@SuppressWarnings("unchecked")
		MVCCIndex<K> result = (MVCCIndex<K>) indexes.get(new MVCCExtractor());
		if (result == null) {
			throw new IllegalArgumentException("No MVCCIndex defined");
		}
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((transactionId == null) ? 0 : transactionId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		@SuppressWarnings("unchecked")
		MVCCLowerFilter<K> other = (MVCCLowerFilter<K>) obj;
		if (transactionId == null) {
			if (other.transactionId != null) {
				return false;
			}
		} else if (!transactionId.equals(other.transactionId)) {
			return false;
		}
		return true;
	}
}
