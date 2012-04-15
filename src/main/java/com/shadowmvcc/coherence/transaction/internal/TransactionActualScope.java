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

import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Actual scope of a new transaction. If the requested timestamp
 * is before the immutability cutoff, then the transaction may be forced to
 * be read only, and the isolation level may be reduced to {@link IsolationLevel.readUncommitted}
 * as this will still give {@link IsolationLevel.serializable} guarantees.
 * If the transaction id also falls within a reaped window, the timestamp will
 * be taken back to the beginning of that window.
 */
@Portable
public class TransactionActualScope {
    
    @PortableProperty(0) private IsolationLevel isolationLevel;
    @PortableProperty(1) private long timestamp;
    @PortableProperty(2) private boolean readonly;

    /**
     *  Default constructor for POF use only.
     */
    public TransactionActualScope() {
        super();
    }
    
    /**
     * @param isolationLevel isolation level to use in the transaction
     * @param timestamp timestamp adjusted to reap window if appropriate
     * @param readonly must the transaction be read only
     */
    public TransactionActualScope(final IsolationLevel isolationLevel,
            final long timestamp, final boolean readonly) {
        super();
        this.isolationLevel = isolationLevel;
        this.timestamp = timestamp;
        this.readonly = readonly;
    }

    /**
     * @return the isolation level to use
     */
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * @return the timestamp to use
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return true if the transaction must be read only
     */
    public boolean isReadonly() {
        return readonly;
    }
    
}