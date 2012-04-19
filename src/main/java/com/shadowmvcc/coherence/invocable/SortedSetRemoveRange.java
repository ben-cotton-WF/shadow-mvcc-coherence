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
import java.util.NavigableSet;
import java.util.TreeSet;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Delete intermediate values between a start and end value. Start and end
 * must be present in the set. Return true if any values were deleted, false if
 * there were no intermediate values and null if start and/or end were not
 * present.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <T> type of the entry being added and of the set
 */
@Portable
public class SortedSetRemoveRange<T extends Comparable<T>> extends AbstractProcessor {

    private static final long serialVersionUID = 8348665851594399438L;

    @PortableProperty (0) private T start;
    @PortableProperty (1) private T end;
    
    /**
     *  Default constructor for POF use only.
     */
    public SortedSetRemoveRange() {
        super();
    }

    /**
     * Constructor.
     * @param start exclusive start of range to remove
     * @param end exclusive end of range to remove
     */
    public SortedSetRemoveRange(final T start, final T end) {
        super();
        this.start = start;
        this.end = end;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object process(final Entry entry) {
        
        NavigableSet<T> theSet;
        
        if (!entry.isPresent()) {
            return null;
        }
        theSet = new TreeSet<T>((Collection<T>) entry.getValue());
        
        if (!theSet.contains(start) || !theSet.contains(end)) {
            return null;
        }
        
        T next = null;
        Boolean changed = Boolean.FALSE;
        do {
            next = theSet.higher(start);
            if (next.compareTo(end) < 0) {
                theSet.remove(next);
                changed = Boolean.TRUE;
            }
        } while (next.compareTo(end) < 0);

        if (changed) {
            entry.setValue(theSet);
        }
        
        return changed;
        
    }

}
