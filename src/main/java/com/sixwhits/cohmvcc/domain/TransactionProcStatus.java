package com.sixwhits.cohmvcc.domain;

/**
 * Transaction status.
 */
public enum TransactionProcStatus {
    /**
     * The transaction has started and is open.
     */
    open,
    /**
     * A commit has been issued on the transaction and update of the
     * related cache entries is in progress.
     */
    committing,
    /**
     * A rollback has been issued on the transaction and removal of
     * the related cache entries is in progress.
     */
    rollingback
}