package com.shadowmvcc.coherence.transaction.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Record registration of a cache name against a manager id. The value
 * object is a collection of String, converted to a set, and the new cachename
 * added to the set
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class CacheRegistration extends AbstractProcessor {

    private static final long serialVersionUID = 3970986555705723580L;
    
    @PortableProperty(0) private String cacheName;
    
    /**
     * Constructor.
     * @param cacheName the name of the cache to register
     */
    public CacheRegistration(final String cacheName) {
        super();
        this.cacheName = cacheName;
    }

    /**
     *  Default constructor for POF use only.
     */
    public CacheRegistration() {
        super();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object process(final Entry entry) {
        Set<String> registeredNames;
        
        if (entry.isPresent()) {
            registeredNames = new HashSet<String>((Collection<String>) entry.getValue());
        } else {
            registeredNames = new HashSet<String>();
        }
        
        registeredNames.add(cacheName);
        
        entry.setValue(registeredNames);
        
        return null;
    }

}
