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

package com.shadowmvcc.coherence.domain;

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
