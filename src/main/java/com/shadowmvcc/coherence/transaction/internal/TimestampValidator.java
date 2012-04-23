package com.shadowmvcc.coherence.transaction.internal;

/**
 * Validation methods to ensure that transactions don't overlap
 * snapshots. We do this by ensuring a sufficiently large gap between the most 
 * recent snapshot that can be created and the oldest transaction that can
 * be created, taking into account the maximum transaction lifetime and the
 * poll interval for cleanup of overdue transactions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TimestampValidator {
    /**
     * Check whether its ok to create a snapshot with this timestamp.
     * @param timestamp the proposed snapshot timestamp
     * @return true if the snapshot is old enough
     */
    boolean isSnapshotAgeValid(long timestamp);
    
    /**
     * Check whether its ok to create a transaction with this timestamp.
     * @param timestamp the proposed transaction timestamp
     * @return true if the transaction is new enough
     */
    boolean isTransactionTimestampValid(long timestamp);

}
