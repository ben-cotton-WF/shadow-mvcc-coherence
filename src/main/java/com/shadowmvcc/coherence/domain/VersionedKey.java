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

import java.io.Serializable;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.cache.KeyAssociation;

/**
 * Key class for the version cache. Contains the logical key, which is also
 * the key of the key cache, and the transaction id, which provides the timestamp ordering.
 * 
 * Implements key association delegating to the logical key so that version cache and key cache entries
 * are always collocated
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical key class
 */
//TODO clean up nomenclature logical/native key, transaction id/timestamp
@Portable
public class VersionedKey<K> implements KeyAssociation, Serializable {

    private static final long serialVersionUID = 8459004703379236862L;

    public static final int POF_KEY = 0;
    @PortableProperty(POF_KEY)
    private K nativeKey;

    public static final int POF_TIMESTAMP = 1;
    @PortableProperty(POF_TIMESTAMP)
    private TransactionId timeStamp;

    /**
     * Default constructor for POF use only.
     */
    public VersionedKey() {
    }

    /**
     * Constructor.
     * @param nativeKey the logical key
     * @param timeStamp the transaction id
     */
    public VersionedKey(final K nativeKey, final TransactionId txTimeStamp) {
        super();
        this.nativeKey = nativeKey;
        this.timeStamp = txTimeStamp;
    }

    /**
     * @return the logical key
     */
    public K getNativeKey() {
        return nativeKey;
    }

    /**
     * @return the transaction id
     */
    public TransactionId getTimeStamp() {
        return timeStamp;
    }

    @Override
    public Object getAssociatedKey() {
        if (nativeKey instanceof KeyAssociation) {
            return ((KeyAssociation) nativeKey).getAssociatedKey();
        } else {
            return nativeKey.hashCode();
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((nativeKey == null) ? 0 : nativeKey.hashCode());
        result = prime * result
                + ((timeStamp == null) ? 0 : timeStamp.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings({"rawtypes" })
        VersionedKey other = (VersionedKey) obj;
        if (nativeKey == null) {
            if (other.nativeKey != null) {
                return false;
            }
        } else if (!nativeKey.equals(other.nativeKey)) {
            return false;
        }
        if (timeStamp == null) {
            if (other.timeStamp != null) {
                return false;
            }
        } else if (!timeStamp.equals(other.timeStamp)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return nativeKey + " ...@" + timeStamp;
    }


}
