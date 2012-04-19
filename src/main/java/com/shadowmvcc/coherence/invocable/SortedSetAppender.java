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

package com.shadowmvcc.coherence.invocable;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import com.shadowmvcc.coherence.pof.SortedSetCodec;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Append an item to the end of a sorted set value object.
 * Returns true if successful, false if the item
 * already exists.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <T> type of the entry being added and of the set
 */
@Portable
public class SortedSetAppender<T extends Comparable<T>> extends AbstractProcessor {

    private static final long serialVersionUID = 8348665851594399438L;

    @PortableProperty (value = 0, codec = SortedSetCodec.class) private SortedSet<T> defaultInitialSet;
    @PortableProperty (1) private T value;
    
    /**
     *  Default constructor for POF use only.
     */
    public SortedSetAppender() {
        super();
    }

    /**
     * Constructor.
     * @param defaultInitialSet initial set if not already present.
     * @param value value to add to the end of the set
     */
    public SortedSetAppender(final SortedSet<T> defaultInitialSet, final T value) {
        super();
        this.defaultInitialSet = defaultInitialSet;
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object process(final Entry entry) {
        
        SortedSet<T> theSet;
        
        if (entry.isPresent()) {
            theSet = new TreeSet<T>((Collection<T>) entry.getValue());
        } else {
            theSet = defaultInitialSet;
        }
        
        if (theSet.contains(value)) {
            return null;
        }
        
        T oldLast = theSet.last();
        
        if (oldLast.compareTo(value) > 0) {
            return null;
        }
        
        theSet.add(value);
        
        entry.setValue(theSet);
        
        return oldLast;
        
    }

}
