package com.sixwhits.cohmvcc.index;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;

/**
 * @author David Whitmarsh from an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 */
@Portable
public class MVCCSurfaceFilter implements IndexAwareFilter, Serializable {

	private static final long serialVersionUID = 5267677476884085089L;

	public static final int POF_TXID = 0;
	@PortableProperty(POF_TXID)
	private TransactionId transactionId;
	
	public MVCCSurfaceFilter() {
		// required 
	}
	
	public MVCCSurfaceFilter(TransactionId transactionId) {
		this.transactionId = transactionId;
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
		MVCCIndex index = getIndex(indexes);
		candidates.retainAll(index.floorSet(candidates, transactionId));
		
		// no further filtering required
		return null;
	}

	@SuppressWarnings("rawtypes")
	private MVCCIndex<?> getIndex(Map indexes) {
		MVCCIndex result = (MVCCIndex) indexes.get(new MVCCExtractor());
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
		MVCCSurfaceFilter other = (MVCCSurfaceFilter) obj;
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
