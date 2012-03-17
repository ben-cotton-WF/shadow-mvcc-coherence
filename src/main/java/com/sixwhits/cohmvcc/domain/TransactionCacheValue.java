package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Cache entry that represents the status of an open transaction. An entry is created in the
 * main transaction cache before any change is made to other caches. This entry is deleted
 * once the transaction has completed committing or rolling back.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class TransactionCacheValue {

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
    };

    @PortableProperty(0)
    private TransactionProcStatus procStatus;
    @PortableProperty(1)
    private long realTimestamp;

    /**
     *  Default constructor for POF use only.
     */
    public TransactionCacheValue() {
        super();
    }

    /**
     * Constructor.
     * @param procStatus the transaction processing status
     * @param realTimestamp the real clock time (not transaction time) that this
     * transaction last changed status or was created.
     */
    public TransactionCacheValue(final TransactionProcStatus procStatus, 
            final long realTimestamp) {
        super();
        this.procStatus = procStatus;
        this.realTimestamp = realTimestamp;
    }

    /**
     * @return the transaction status
     */
    public TransactionProcStatus getProcStatus() {
        return procStatus;
    }

    /**
     * @return the transaction real-time timestamp
     */
    public long getRealTimestamp() {
        return realTimestamp;
    }




}
