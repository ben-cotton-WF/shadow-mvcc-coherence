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

import com.shadowmvcc.coherence.domain.VersionedKey;

/**
 * Exception raised when insert of a new version fails because of an insert with a
 * later timestamp.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class FutureReadException extends AbstractMVCCException {

    private static final long serialVersionUID = 7054631620027498791L;

    /**
     * @param key version cache key of the entry
     * @param message the message
     */
    public FutureReadException(final VersionedKey<?> key, final String message) {
        super(key, message);
    }

    /**
     * @param key version cache key of the entry
     */
    public FutureReadException(final VersionedKey<?> key) {
        super(key);
    }

    /**
     * Constructor.
     */
    public FutureReadException() {
        super();
    }


}
