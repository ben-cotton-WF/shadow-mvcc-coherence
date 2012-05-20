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

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ValueUpdater;

/**
 * Read-only wrapper for a version cache {@code BinaryEntry} to present it as
 * a logical cache entry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ReadOnlyEntryWrapper extends AbstractEntryWrapper {


    /**
     * @param parentEntry the version cache entry
     * @param transactionId transaction id of the enclosing transaction
     * @param isolationLevel isolation level of the enclosing transaction
     * @param cacheName the cache name
     */
    public ReadOnlyEntryWrapper(final BinaryEntry parentEntry, final TransactionId transactionId,
            final IsolationLevel isolationLevel, final CacheName cacheName) {
        super(parentEntry, transactionId, isolationLevel, cacheName,
                parentEntry.getBackingMapContext().getManagerContext());
    }

    @Override
    public Binary getBinaryValue() {
        return getOriginalBinaryValue();
    }

    @Override
    public void updateBinaryValue(final Binary binary) {
        throw new UnsupportedOperationException("Read only entry");
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Object getValue() {
        return getOriginalValue();
    }

    @Override
    public Object setValue(final Object obj) {
        throw new UnsupportedOperationException("Read only entry");
    }

    @Override
    public void setValue(final Object obj, final boolean flag) {
        throw new UnsupportedOperationException("Read only entry");
    }

    @Override
    public void update(final ValueUpdater valueupdater, final Object obj) {
        throw new UnsupportedOperationException("Read only entry");
    }

    @Override
    public void remove(final boolean flag) {
        throw new UnsupportedOperationException("Read only entry");
    }

    @Override
    public boolean isRemove() {
        return false;
    }

}
