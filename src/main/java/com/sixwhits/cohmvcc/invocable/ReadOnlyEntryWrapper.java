package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
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
     * @param transactionId the transaction id
     * @param isolationLevel the isolation level
     * @param cacheName the cache name
     */
    public ReadOnlyEntryWrapper(final BinaryEntry parentEntry, 
            final TransactionId transactionId, final IsolationLevel isolationLevel, 
            final CacheName cacheName) {
        super(parentEntry, transactionId, isolationLevel, cacheName);
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
