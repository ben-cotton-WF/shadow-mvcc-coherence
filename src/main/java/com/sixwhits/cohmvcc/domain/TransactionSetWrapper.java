package com.sixwhits.cohmvcc.domain;

import java.util.NavigableSet;

import com.sixwhits.cohmvcc.pof.SortedSetCodec;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Wrapper for the set of read timestamps that are the
 * value object of a key cache.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class TransactionSetWrapper {
    @PortableProperty(value = 0, codec = SortedSetCodec.class)
    private NavigableSet<TransactionId> transactionIdSet;

    /**
     * @return the set of read transaction ids
     */
    public NavigableSet<TransactionId> getTransactionIdSet() {
        return transactionIdSet;
    }

    /**
     * @param transactionIdSet the set of read transaction ids
     */
    public void setTransactionIdSet(final NavigableSet<TransactionId> transactionIdSet) {
        this.transactionIdSet = transactionIdSet;
    }

}
