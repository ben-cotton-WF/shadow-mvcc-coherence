package com.shadowmvcc.coherence.domain;

/**
 * Transaction isolation levels, follows ISO convention with extensions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public enum IsolationLevel {
    /**
     * Reads uncommitted values. Will never wait for commit
     */
    readUncommitted, 
    /**
     * Read only committed values. Wait for uncommitted entries
     */
    readCommitted, 
    /**
     * Read committed and insert a read marker to prevent updates with an earlier timestamp.
     */
    repeatableRead, 
    /**
     * Repeatable read, and record filter criteria against the timestamp to prevent updates with
     * an earlier timestamp that would match the filter criteria.
     */
    serializable, 
    /**
     * Update only operation with out reading. Allows the optimisation of not having to check
     * for uncommitted changes
     */
    readProhibited, 
//    readCommittedNoWait, 
//    eternal
}
