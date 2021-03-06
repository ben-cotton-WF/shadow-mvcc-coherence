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

import static com.shadowmvcc.coherence.domain.TransactionProcStatus.committing;
import static com.shadowmvcc.coherence.domain.TransactionProcStatus.rollingback;

import com.shadowmvcc.coherence.domain.TransactionCacheValue;
import com.shadowmvcc.coherence.domain.TransactionProcStatus;
import com.shadowmvcc.coherence.transaction.TransactionException;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Update the state of a transaction cache entry to committing or rollingback.
 * Throws an exception if the transaction does not exist or is not in open state.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class TransactionStateUpdater extends AbstractProcessor {
    
    private static final long serialVersionUID = 1691302505312809338L;
    private static final PofExtractor STATUS_X = 
            new PofExtractor(TransactionProcStatus.class, TransactionCacheValue.POF_STATUS);
    
    public static final TransactionStateUpdater COMMIT = new TransactionStateUpdater(committing);
    public static final TransactionStateUpdater ROLLBACK = new TransactionStateUpdater(rollingback);

    @PortableProperty(0) private TransactionProcStatus newStatus;

    /**
     *  Default constructor for POF use only.
     */
    public TransactionStateUpdater() {
        super();
    }

    /**
     * @param newStatus status to set the transaction to.
     */
    public TransactionStateUpdater(final TransactionProcStatus newStatus) {
        super();
        this.newStatus = newStatus;
    }

    @Override
    public Object process(final Entry entry) {

        if (!entry.isPresent()) {
            throw new TransactionException("Transaction " + entry.getKey() + " does not exist");
        }
        
        TransactionProcStatus currentStatus = (TransactionProcStatus) STATUS_X.extractFromEntry(entry);
        if (currentStatus != TransactionProcStatus.open) {
            throw new TransactionException("Transaction " + entry.getKey() + " is " + currentStatus);
        }
        
        entry.setValue(new TransactionCacheValue(newStatus, System.currentTimeMillis()));
        
        return null;

    }

}
