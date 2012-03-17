package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
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
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     * @param cacheName the cache name
     */
    public ReadWriteEntryWrapper(final BinaryEntry parentEntry,
            final TransactionId transactionId, final IsolationLevel isolationLevel, final CacheName cacheName) {
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
     * @see com.sixwhits.cohmvcc.invocable.EntryWrapper#isRemove()
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