package com.shadowmvcc.coherence.invocable;

import com.shadowmvcc.coherence.domain.Constants;
import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Wrapper for a version cache entry to make it look like a logical cache entry.
 * Converts the key from VersionedKey<K> to K
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class VersionCacheBinaryEntryWrapper implements BinaryEntry {

    private final BinaryEntry underlying;

    /**
     * Constructor.
     * @param underlying the underlying version cache entry
     */
    public VersionCacheBinaryEntryWrapper(final BinaryEntry underlying) {
        super();
        this.underlying = underlying;
    }

    @Override
    public Object getKey() {
        return Constants.KEYEXTRACTOR.extractFromEntry(underlying);
    }

    @Override
    public Object getValue() {
        return getBackingMapContext().getManagerContext().getValueFromInternalConverter().convert(getBinaryValue());
    }

    @Override
    public Object setValue(final Object obj) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public void setValue(final Object obj, final boolean flag) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public void update(final ValueUpdater valueupdater, final Object obj) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public boolean isPresent() {
        return underlying.isPresent();
    }

    @Override
    public void remove(final boolean flag) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public Object extract(final ValueExtractor valueextractor) {
        if (valueextractor instanceof PofExtractor) {
            return ((PofExtractor) valueextractor).extractFromEntry(this);
        } else {
            return valueextractor.extract(getValue());
        }
    }

    @Override
    public Binary getBinaryKey() {
        // TODO get rid of convert from/to binary
        return (Binary) getBackingMapContext().getManagerContext().getKeyToInternalConverter().convert(getKey());
    }

    @Override
    public Binary getBinaryValue() {
        return underlying.getBinaryValue();
    }

    @Override
    public Serializer getSerializer() {
        return underlying.getSerializer();
    }

    @Override
    public BackingMapManagerContext getContext() {
        return underlying.getContext();
    }

    @Override
    public void updateBinaryValue(final Binary binary) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public Object getOriginalValue() {
        return getValue();
    }

    @Override
    public Binary getOriginalBinaryValue() {
        return getBinaryValue();
    }

    @Override
    public ObservableMap getBackingMap() {
        throw new UnsupportedOperationException("fake entry");
    }

    @Override
    public BackingMapContext getBackingMapContext() {
        return underlying.getBackingMapContext();
    }

    @Override
    public void expire(final long l) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public boolean isReadOnly() {
        return underlying.isReadOnly();
    }

}
