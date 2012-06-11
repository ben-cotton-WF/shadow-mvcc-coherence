package com.shadowmvcc.coherence.transaction.internal;

/**
 * Interface for classes to be notified of transaction expiry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TransactionExpiryMonitor {
    /**
     * Set the transaction to expired.
     */
    void setTransactionExpired();

}
