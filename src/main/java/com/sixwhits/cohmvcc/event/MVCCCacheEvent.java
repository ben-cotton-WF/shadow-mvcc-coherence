package com.sixwhits.cohmvcc.event;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.net.cache.CacheEvent;
import com.tangosol.util.ObservableMap;

/**
 *
 * Extends {@code CacheEvent} to provide additional transaction context.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCCacheEvent extends CacheEvent {

    /**
     * Commit status of the entry after the event.
     */
    public enum CommitStatus { open, commit, rollback }
    private static final long serialVersionUID = -1015113502920893252L;
    private final CommitStatus commitStatus;
    private final TransactionId transactionId;

    /**
     * @param map the underlying version cache
     * @param nId logical event type - insert, update, delete
     * @param oKey logical key
     * @param oValueOld value of the previous version
     * @param oValueNew value of the current version
     * @param synthetic synthetic event
     * @param transactionId transaction id
     * @param commitStatus commit status
     */
    public MVCCCacheEvent(final ObservableMap map, final int nId, final Object oKey, 
            final Object oValueOld, final Object oValueNew, final boolean synthetic,
            final TransactionId transactionId, final CommitStatus commitStatus) {
        super(map, nId, oKey, oValueOld, oValueNew, synthetic);
        this.commitStatus = commitStatus;
        this.transactionId = transactionId;
    }

    /**
     * @return the transaction Id responsible for the change
     */
    public TransactionId getTransactionId() {
        return transactionId;
    }

    /**
     * @return the commit status of the event
     */
    public CommitStatus getCommitStatus() {
        return commitStatus;
    }



}
