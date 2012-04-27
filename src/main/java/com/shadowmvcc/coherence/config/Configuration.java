package com.shadowmvcc.coherence.config;

/**
 * Central interface for configuration information. Used to isolate
 * the actual configuration implementation. We might want to have a means of
 * extend clients getting the configuration from the cluster.
 * 
 * Not good that this introduces coupling between otherwise unrelated elements
 * but Coherence design makes an injection approach problematic. Consider splitting
 * into separate interfaces.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface Configuration {
    /**
     * Get the maximum age relative to cluster time that a new
     * transaction can have.
     * @return the maxiumum age in milliseconds
     */
    long getMaximumTransactionAge();
    /**
     * Get the minimum age for a new snapshot. This must be
     * greater than maximum transaction age + open transaction timeout
     * + transaction completion timeout + transaction poll interval
     * + a safety margin, so that snapshots and open transactions can
     * never overlap. 
     * @return minimum age in milliseconds
     */
    long getMinimumSnapshotAge();
    /**
     * Get the name of the invocation service to use to distribute processing
     * in the cluster.
     * @return the service name
     */
    String getInvocationServiceName();
    /**
     * Get the transaction timeout. Older transactions will be rolled back
     * @return the timeout in milliseconds
     */
    long getOpenTransactionTimeout();
    /**
     * Get the transaction completion timeout. If a transaction has been
     * committing or rolling back for longer than this time, the client is
     * assumed to have died and the transaction monitor will complete
     * the commit or rollback.
     * @return the timeout in milliseconds
     */
    long getTransactionCompletionTimeout();
    /**
     * Get the poll interval for transaction monitor thread. This is how often
     * the monitor checks for timed out open or not completed transactions.
     * @return the poll interval in milliseconds
     */
    long getTransactionPollInterval();
}
