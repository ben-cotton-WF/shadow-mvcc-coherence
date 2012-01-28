package com.sixwhits.cohmvcc.index;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.MapIndex;
import com.tangosol.util.extractor.IndexAwareExtractor;

/**
 * @author David Whitmarsh from an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class MVCCExtractor implements IndexAwareExtractor, PortableObject, Serializable {
	
	private static final long serialVersionUID = 4263977259382277921L;

	public MVCCExtractor() {
	}

	@Override
	public Object extract(Object paramObject) {
		throw new UnsupportedOperationException("Can only be used for indexing");
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MapIndex createIndex(boolean ordered, Comparator comparator, Map indexes, BackingMapContext bmc) {
		if (indexes.containsKey(this)) {
			return null;
		}
		else {
			MVCCIndex index = new MVCCIndex(bmc);
			indexes.put(this, index);
			return index;
		}
	}

	@Override
	@SuppressWarnings("rawtypes")
	public MapIndex destroyIndex(Map indexes) {
		return (MapIndex) indexes.remove(this);
	}

	@Override
	public int hashCode() {
		return 31;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		return true;
	}

	@Override
	public void readExternal(PofReader in) throws IOException {
	}

	@Override
	public void writeExternal(PofWriter out) throws IOException {
	}
}
