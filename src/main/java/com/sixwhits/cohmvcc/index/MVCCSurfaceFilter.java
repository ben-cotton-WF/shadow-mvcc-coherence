package com.sixwhits.cohmvcc.index;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
public class MVCCSurfaceFilter<K> implements IndexAwareFilter, Serializable {

	private static final long serialVersionUID = 5267677476884085089L;

	public static final int POF_TXID = 0;
	@PortableProperty(POF_TXID)
	private TransactionId transactionId;
	public static final int POF_KEYSET = 1;
	@PortableProperty(POF_KEYSET)
	private Collection<K> keySet;
	public static final int POF_FILTER = 2;
	@PortableProperty(POF_FILTER)
	private Filter filter = null;
	
	public MVCCSurfaceFilter() {
		// required 
	}
	
	public MVCCSurfaceFilter(TransactionId transactionId) {
		super();
		this.transactionId = transactionId;
	}

	public MVCCSurfaceFilter(TransactionId transactionId, Collection<K> keySet) {
		super();
		this.transactionId = transactionId;
		this.keySet = Collections.unmodifiableCollection(keySet);
	}

	public MVCCSurfaceFilter(TransactionId transactionId, Filter filter) {
		super();
		this.transactionId = transactionId;
		this.filter = filter;
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
		Filter result = null;
		MVCCIndex<K> index = getIndex(indexes);
		if (keySet != null) {
			Set<Binary> floorBinaryKeys = new HashSet<Binary>(keySet.size());
			for (K key : keySet) {
				Binary keyFloor = index.floor(key, transactionId);
				if (keyFloor != null) {
					floorBinaryKeys.add(keyFloor);
				}
			}
			candidates.retainAll(floorBinaryKeys);
		} else {
			if (filter != null && filter instanceof IndexAwareFilter) {
				result = ((IndexAwareFilter)filter).applyIndex(indexes, candidates);
			} else {
				result = filter;
			}
			candidates.retainAll(index.floorSet(candidates, transactionId));
		}
		
		// no further filtering required
		return result == null ? null : new FilterWrapper(result);
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
		MVCCSurfaceFilter<K> other = (MVCCSurfaceFilter<K>) obj;
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
