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

package com.shadowmvcc.coherence.cache.internal;

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
