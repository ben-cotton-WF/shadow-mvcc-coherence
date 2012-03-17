package com.sixwhits.cohmvcc.cache.internal;

import java.util.Map;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * {@code EntryProcessor} implementation to put a value taken from a map.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
@Portable
public class PutAllProcessor<K, V> extends AbstractProcessor {

    private static final long serialVersionUID = -5621228179782770648L;

    public static final int POF_VALUEMAP = 0;
    @PortableProperty(POF_VALUEMAP)
    private Map<K, V> valueMap;

    /**
     * Default constructor for POF use only.
     */
    public PutAllProcessor() {
        super();
    }

    /**
     * @param valueMap map containing value to put
     */
    public PutAllProcessor(final Map<K, V> valueMap) {
        super();
        this.valueMap = valueMap;
    }

    @Override
    public Object process(final Entry entry) {
        if (valueMap.containsKey(entry.getKey())) {
            entry.setValue(valueMap.get(entry.getKey()), false);
        }
        return null;
    }
}
