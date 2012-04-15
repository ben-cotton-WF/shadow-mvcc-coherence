package com.shadowmvcc.coherence.domain;

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

    public static final int POF_STATUS = 0;
    @PortableProperty(POF_STATUS) private TransactionProcStatus procStatus;
    public static final int POF_REALTIME = 1;
    @PortableProperty(POF_REALTIME) private long realTimestamp;

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
