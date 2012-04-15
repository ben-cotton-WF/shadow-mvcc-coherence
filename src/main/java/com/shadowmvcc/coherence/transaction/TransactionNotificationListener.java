package com.shadowmvcc.coherence.transaction;

/**
 * Listener to receive notifications when a transaction has been committed or rolled back.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TransactionNotificationListener {
    /**
     * @param transaction the transaction that has completed
     */
    void transactionComplete(Transaction transaction);
}
