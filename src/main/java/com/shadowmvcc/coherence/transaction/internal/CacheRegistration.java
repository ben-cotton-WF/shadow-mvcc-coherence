/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

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
