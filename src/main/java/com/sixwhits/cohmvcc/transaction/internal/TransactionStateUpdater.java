package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.TransactionProcStatus.committing;
import static com.sixwhits.cohmvcc.domain.TransactionProcStatus.rollingback;

import com.sixwhits.cohmvcc.domain.TransactionCacheValue;
import com.sixwhits.cohmvcc.domain.TransactionProcStatus;
import com.sixwhits.cohmvcc.transaction.TransactionException;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Update the state of a transaction cache entry to committing or rollingback.
 * Throws an exception if the transaction does not exist or is not in open state.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class TransactionStateUpdater extends AbstractProcessor {
    
    private static final long serialVersionUID = 1691302505312809338L;
    private static final PofExtractor STATUS_X = 
            new PofExtractor(TransactionProcStatus.class, TransactionCacheValue.POF_STATUS);
    
    public static final TransactionStateUpdater COMMIT = new TransactionStateUpdater(committing);
    public static final TransactionStateUpdater ROLLBACK = new TransactionStateUpdater(rollingback);

    @PortableProperty(0) private TransactionProcStatus newStatus;

    /**
     *  Default constructor for POF use only.
     */
    public TransactionStateUpdater() {
        super();
    }

    /**
     * @param newStatus status to set the transaction to.
     */
    public TransactionStateUpdater(final TransactionProcStatus newStatus) {
        super();
        this.newStatus = newStatus;
    }

    @Override
    public Object process(final Entry entry) {

        if (!entry.isPresent()) {
            throw new TransactionException("Transaction " + entry.getKey() + " does not exist");
        }
        
        TransactionProcStatus currentStatus = (TransactionProcStatus) STATUS_X.extractFromEntry(entry);
        if (currentStatus != TransactionProcStatus.open) {
            throw new TransactionException("Transaction " + entry.getKey() + " is " + currentStatus);
        }
        
        entry.setValue(new TransactionCacheValue(newStatus, System.currentTimeMillis()));
        
        return null;

    }

}
