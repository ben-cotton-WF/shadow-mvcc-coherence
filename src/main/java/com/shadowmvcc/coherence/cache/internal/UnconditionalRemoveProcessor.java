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

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * {@code EntryProcessor} to remove a cache entry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class UnconditionalRemoveProcessor extends AbstractProcessor {

    private static final long serialVersionUID = -1589869423220481276L;
    public static final int POF_RETURN = 0;
    @PortableProperty(POF_RETURN)
    private boolean returnPrior = true;

    /**
     * Default constructor for POF use only.
     */
    public UnconditionalRemoveProcessor() {
        super();
    }

    /**
     * @param returnPrior return the old value if true, else null
     */
    public UnconditionalRemoveProcessor(final boolean returnPrior) {
        super();
        this.returnPrior = returnPrior;
    }

    @Override
    public Object process(final Entry entry) {
        entry.remove(false);
        return returnPrior ? entry.getValue() : null;
    }
}
