package com.shadowmvcc.coherence.transaction.internal;

import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Actual scope of a new transaction. If the requested timestamp
 * is before the immutability cutoff, then the transaction may be forced to
 * be read only, and the isolation level may be reduced to {@link IsolationLevel.readUncommitted}
 * as this will still give {@link IsolationLevel.serializable} guarantees.
 * If the transaction id also falls within a reaped window, the timestamp will
 * be taken back to the beginning of that window.
 */
@Portable
public class TransactionActualScope {
    
    @PortableProperty(0) private IsolationLevel isolationLevel;
    @PortableProperty(1) private long timestamp;
    @PortableProperty(2) private boolean readonly;

    /**
     *  Default constructor for POF use only.
     */
    public TransactionActualScope() {
        super();
    }
    
    /**
     * @param isolationLevel isolation level to use in the transaction
     * @param timestamp timestamp adjusted to reap window if appropriate
     * @param readonly must the transaction be read only
     */
    public TransactionActualScope(final IsolationLevel isolationLevel,
            final long timestamp, final boolean readonly) {
        super();
        this.isolationLevel = isolationLevel;
        this.timestamp = timestamp;
        this.readonly = readonly;
    }

    /**
     * @return the isolation level to use
     */
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * @return the timestamp to use
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return true if the transaction must be read only
     */
    public boolean isReadonly() {
        return readonly;
    }
    
}