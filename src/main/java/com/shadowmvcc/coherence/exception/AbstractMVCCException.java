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

package com.shadowmvcc.coherence.exception;

import java.io.IOException;

import com.shadowmvcc.coherence.domain.VersionedKey;
import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableException;

/**
 * Base class for MVCC exceptions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public abstract class AbstractMVCCException extends PortableException {

    private static final long serialVersionUID = 2338727005397469974L;
    private VersionedKey<?> key;

    /**
     * @return the version cache key the exception relates to
     */
    public VersionedKey<?> getKey() {
        return key;
    }

    /**
     * Default constructor for POF use only.
     */
    public AbstractMVCCException() {
        super();
    }

    /**
     * Constructor.
     * @param key key this exception relates to
     */
    public AbstractMVCCException(final VersionedKey<?> key) {
        super();
        this.key = key;
    }

    /**
     * Constructor.
     * @param key the key
     * @param message the message
     */
    public AbstractMVCCException(final VersionedKey<?> key, final String message) {
        super(message);
        this.key = key;
    }

    @Override
    public void readExternal(final PofReader in) throws IOException {
        super.readExternal(in);
        key = (VersionedKey<?>) in.readObject(1000);
    }

    @Override
    public void writeExternal(final PofWriter out) throws IOException {
        super.writeExternal(out);
        out.writeObject(1000, key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
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
        AbstractMVCCException other = (AbstractMVCCException) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        return true;
    }


}
