package com.shadowmvcc.coherence.transaction.internal;

import com.shadowmvcc.coherence.domain.TransactionCacheValue;
import com.shadowmvcc.coherence.domain.TransactionProcStatus;
import com.tangosol.util.AbstractMapListener;
import com.tangosol.util.MapEvent;

/**
 * Mediate between MapListener and interest in expired transaction.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class TransactionExpiryListener extends AbstractMapListener {

    private final TransactionExpiryMonitor expiryMonitor;
    
    /**
     * Constructor.
     * @param expiryMonitor bean to update is transaction has expired
     */
    public TransactionExpiryListener(final TransactionExpiryMonitor expiryMonitor) {
        super();
        this.expiryMonitor = expiryMonitor;
    }

    @Override
    public void entryUpdated(final MapEvent mapevent) {
        TransactionCacheValue transactionStatus = (TransactionCacheValue) mapevent.getNewValue();
        if (transactionStatus.getProcStatus() == TransactionProcStatus.rollingback) {
            expiryMonitor.setTransactionExpired();
        }
    }

}
