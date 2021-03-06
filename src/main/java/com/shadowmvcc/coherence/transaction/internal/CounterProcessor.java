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

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Get a value from a counter. Simply increments and returns a counter
 * starting from zero if the entry does not exist
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class CounterProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 44873576034046440L;
    public static final CounterProcessor INSTANCE = new CounterProcessor();
    
    @Override
    public Object process(final Entry entry) {
        Integer managerId;
        
        if (!entry.isPresent()) {
            managerId = 0;
        } else {
            managerId = (Integer) entry.getValue();
        }
        managerId++;
        entry.setValue(managerId);
        
        return managerId;
    }

}
