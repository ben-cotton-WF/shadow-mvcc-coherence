package com.shadowmvcc.coherence.invocable;

import java.util.Collection;
import java.util.Collections;

import com.shadowmvcc.coherence.cache.CacheName;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * EntryProcessor for testing partition local transactions
 * Invoked against one cache it will set or increment the integer value of that cache
 * and will also decrement or set the integer value of a second cache using the same
 * key.
 * 
 * Should work with either MVCC or simple caches.
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class PartitionLocalTransactionProcessor extends AbstractProcessor implements MultiCacheProcessor {
    
    private static final long serialVersionUID = 7181400992941715518L;
    @PortableProperty(0) private String otherCacheName;
    @PortableProperty(1) private boolean otherIsMVCC;

    /**
     *  Default constructor for POF use only.
     */
    public PartitionLocalTransactionProcessor() {
        super();
    }

    /**
     * Constructor.
     * @param otherCacheName name of the other cache
     * @param otherIsMVCC true if the other cache is an MVCC cache
     */
    public PartitionLocalTransactionProcessor(final String otherCacheName, final boolean otherIsMVCC) {
        super();
        this.otherCacheName = otherCacheName;
        this.otherIsMVCC = otherIsMVCC;
    }

    @Override
    public Object process(final Entry entry) {
        
        BinaryEntry binaryEntry = (BinaryEntry) entry;
        
        Integer value = 0;
        if (entry.isPresent()) {
            value = (Integer) entry.getValue();
        }
        
        value++;
        
        entry.setValue(value);
        
        BackingMapContext otherContext = binaryEntry.getContext().getBackingMapContext(otherCacheName);
        Entry otherEntry = otherContext.getBackingMapEntry(binaryEntry.getBinaryKey());
        
        Integer otherValue = 0;
        if (otherEntry.isPresent()) {
            otherValue = (Integer) otherEntry.getValue();
        }
        
        otherValue--;
        
        otherEntry.setValue(otherValue);
        
        return otherValue;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<CacheName> getReferencedMVCCCacheNames() {
        return (Collection<CacheName>) (otherIsMVCC
                ? Collections.singleton(new CacheName(otherCacheName)) : Collections.emptyList());
    }

}
