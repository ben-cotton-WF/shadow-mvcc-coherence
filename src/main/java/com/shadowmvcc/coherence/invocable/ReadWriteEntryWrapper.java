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
import com.tangosol.util.extractor.PofUpdater;

/**
 * Wrapper for a {@code BinaryEntry} that presents a version cache entry
 * as a logical cache entry. Used in a read-write context, e.g. EntryProcessor wrapper.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ReadWriteEntryWrapper extends AbstractEntryWrapper implements EntryWrapper {

    private boolean delete = false;
    private Binary newBinaryValue;

    /**
     * @param parentEntry the version cache entry
     * @param transactionId transaction id of the enclosing transaction
     * @param isolationLevel isolation level of the enclosing transaction
     * @param cacheName the cache name
     */
    public ReadWriteEntryWrapper(final BinaryEntry parentEntry, final TransactionId transactionId,
            final IsolationLevel isolationLevel, final CacheName cacheName) {
        super(parentEntry, transactionId, isolationLevel, cacheName);
    }

    @Override
    public Object getValue() {
        Binary binaryValue = getBinaryValue();
        return binaryValue == null ? null : getContext().getValueFromInternalConverter().convert(binaryValue);
    }

    @Override
    public Object setValue(final Object obj) {
        Object result = getValue();
        newBinaryValue = (Binary) getBackingMapContext().getManagerContext().getValueToInternalConverter().convert(obj);
        return result;
    }

    @Override
    public void setValue(final Object obj, final boolean flag) {
        newBinaryValue = (Binary) getBackingMapContext().getManagerContext().getValueToInternalConverter().convert(obj);
    }

    @Override
    public void update(final ValueUpdater valueupdater, final Object obj) {
        if (valueupdater instanceof PofUpdater) {
            ((PofUpdater) valueupdater).updateEntry(this, obj);
        } else {
            valueupdater.update(getValue(), obj);
        }
    }

    @Override
    public void remove(final boolean isSynthetic) {
        delete = true;
    }

    @Override
    public Binary getBinaryValue() {
        return newBinaryValue == null ? getOriginalBinaryValue() : newBinaryValue;
    }

    @Override
    public void updateBinaryValue(final Binary binary) {
        newBinaryValue = binary;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    /* (non-Javadoc)
     * @see com.shadowmvcc.coherence.invocable.EntryWrapper#isRemove()
     */
    @Override
    public boolean isRemove() {
        return delete;
    }

    /**
     * @return the modified binary value
     */
    public Binary getNewBinaryValue() {
        return newBinaryValue;
    }
}