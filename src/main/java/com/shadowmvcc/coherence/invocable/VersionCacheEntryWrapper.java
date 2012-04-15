package com.shadowmvcc.coherence.invocable;


import com.shadowmvcc.coherence.domain.VersionedKey;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueUpdater;

/**
 * Wrapper around a single version cache entry to make it look
 * like a logical cache entry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> logical key type
 * @param <V> version type
 */
public class VersionCacheEntryWrapper<K, V> implements Entry {

    private final Entry underlying;

    /**
     * Constructor.
     * @param underlying version cache entry
     */
    public VersionCacheEntryWrapper(final Entry underlying) {
        super();
        this.underlying = underlying;
    }

    @SuppressWarnings("unchecked")
    @Override
    public K getKey() {
        return ((VersionedKey<K>) underlying.getKey()).getNativeKey();
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getValue() {
        return (V) underlying.getValue();
    }

    @Override
    public V setValue(final Object obj) {
        throw new UnsupportedOperationException("read only entry");
    }

    @Override
    public Object extract(final ValueExtractor valueextractor) {
        return valueextractor.extract(getValue());
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

}
