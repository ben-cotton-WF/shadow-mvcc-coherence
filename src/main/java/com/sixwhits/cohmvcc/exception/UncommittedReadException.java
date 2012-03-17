package com.sixwhits.cohmvcc.exception;

import com.sixwhits.cohmvcc.domain.VersionedKey;

/**
 * Exception indicating an uncommitted read.
 * 
 * TODO i dthis still needed?
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class UncommittedReadException extends AbstractMVCCException {

    private static final long serialVersionUID = 7617017177058160058L;

    /**
     * @param key version cache key of the entry
     * @param message the message
     */
    public UncommittedReadException(final VersionedKey<?> key, final String message) {
        super(key, message);
    }

    /**
     * @param key version cache key of the entry
     */
    public UncommittedReadException(final VersionedKey<?> key) {
        super(key);
    }

    /**
     * Constructor.
     */
    public UncommittedReadException() {
        super();
    }


}
