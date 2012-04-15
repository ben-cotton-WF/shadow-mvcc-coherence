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
