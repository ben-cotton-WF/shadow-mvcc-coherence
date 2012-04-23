package com.shadowmvcc.coherence.exception;

import com.tangosol.io.pof.PortableException;

/**
 * Exception relating to creating or coalescing snapshots.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SnapshotCreationException extends PortableException {

    private static final long serialVersionUID = -5499408859764965545L;

    /**
     * Default constructor.
     */
    public SnapshotCreationException() {
    }

    /**
     * Constructor.
     * @param sMessage the message
     */
    public SnapshotCreationException(final String sMessage) {
        super(sMessage);
    }

    /**
     * Constructor.
     * @param e the cause
     */
    public SnapshotCreationException(final Throwable e) {
        super(e);
    }

    /**
     * Constructor.
     * @param sMessage the message
     * @param e the cause
     */
    public SnapshotCreationException(final String sMessage, final Throwable e) {
        super(sMessage, e);
    }


}
