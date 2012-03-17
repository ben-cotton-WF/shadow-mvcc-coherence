package com.sixwhits.cohmvcc.transaction;

/**
 * Exception relating to transaction operations.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class TransactionException extends RuntimeException {

    private static final long serialVersionUID = 8655722432512244531L;

    /**
     * Default constructor.
     */
    public TransactionException() {
        super();
    }

    /**
     * @param message the message
     * @param cause underlying exception cause
     */
    public TransactionException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message the message
     */
    public TransactionException(final String message) {
        super(message);
    }

    /**
     * @param cause underlying exception cause
     */
    public TransactionException(final Throwable cause) {
        super(cause);
    }

}
