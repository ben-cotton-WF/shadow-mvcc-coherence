package com.shadowmvcc.coherence.index;

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
 * Extractor used to build the MVCCIndex, and to identify the index in the backing map context.
 * 
 * @author David Whitmarsh from an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class MVCCExtractor implements IndexAwareExtractor, PortableObject, Serializable {
    
    private static final long serialVersionUID = 4263977259382277921L;
    
    public static final MVCCExtractor INSTANCE = new MVCCExtractor();

    /**
     * Constructor.
     */
    public MVCCExtractor() {
    }

    @Override
    public Object extract(final Object paramObject) {
        throw new UnsupportedOperationException("Can only be used for indexing");
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public MapIndex createIndex(final boolean ordered,
            final Comparator comparator, final Map indexes, final BackingMapContext bmc) {
        if (indexes.containsKey(this)) {
            return null;
        } else {
            MVCCIndex index = new MVCCIndex(bmc);
            indexes.put(this, index);
            return index;
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public MapIndex destroyIndex(final Map indexes) {
        return (MapIndex) indexes.remove(this);
    }

    @Override
    public int hashCode() {
        return 31;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public void readExternal(final PofReader in) throws IOException {
    }

    @Override
    public void writeExternal(final PofWriter out) throws IOException {
    }
}
