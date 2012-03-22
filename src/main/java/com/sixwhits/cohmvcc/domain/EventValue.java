package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Class to encapsulate the cache value, together with its commit and delete decorations. Native Coherence
 * events do not allow access to decorations so we must use a {@code MapEventTransformer} to
 * enrich into one of these events for propagation to the actual {@code MapListener}
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <V> the cache value type
 */
@Portable
public class EventValue<V> {

    @PortableProperty(0) private boolean committed;
    @PortableProperty(1) private boolean deleted;
    @PortableProperty(2) private V value;

    /**
     *  Default constructor for POF use only.
     */
    public EventValue() {
        super();
    }

    /**
     * Constructor.
     * @param committed is the value committed
     * @param deleted is the value deleted
     * @param value the cache value
     */
    public EventValue(final boolean committed, final boolean deleted, final V value) {
        super();
        this.committed = committed;
        this.deleted = deleted;
        this.value = value;
    }

    /**
     * Has the value been committed?
     * @return true if committed
     */
    public boolean isCommitted() {
        return committed;
    }

    /**
     * Has the value been deleted?
     * @return true if deleted
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * Get the value of the cache entry.
     * @return the value
     */
    public V getValue() {
        return value;
    }




}
