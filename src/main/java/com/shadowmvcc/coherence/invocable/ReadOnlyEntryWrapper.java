package com.shadowmvcc.coherence.invocable;

import com.shadowmvcc.coherence.cache.CacheName;
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
     * @param priorBinaryEntry prior version binary entry
     * @param cacheName the cache name
     */
    public ReadOnlyEntryWrapper(final BinaryEntry parentEntry, final BinaryEntry priorBinaryEntry,
            final CacheName cacheName) {
        super(parentEntry, priorBinaryEntry, cacheName);
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
