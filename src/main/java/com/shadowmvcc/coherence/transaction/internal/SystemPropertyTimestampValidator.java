package com.shadowmvcc.coherence.transaction.internal;

import com.tangosol.net.CacheFactory;


/**
 * Implementation of timestamp validator initialised from
 * system properties.
 * 
 * TODO integrate with monitor config and ensure that snapshot age cannot overlap
 * open transactions
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SystemPropertyTimestampValidator implements TimestampValidator {
    
    private static final long MAXTRANSACTIONAGE;
    private static final long MINIMUMSNAPSHOTAGE;
    
    static {
        String ssAgeString = System.getProperty("shadowmvcc.minsnapshotage");
        MINIMUMSNAPSHOTAGE = ssAgeString == null ? 3600000 : Long.parseLong(
                System.getProperty("shadowmvcc.minsnapshotage"));
        String txAgeString = System.getProperty("shadowmvcc.maxtransactionage");
        MAXTRANSACTIONAGE = txAgeString == null ? 300 : Long.parseLong(txAgeString);
    }
    

    @Override
    public boolean isSnapshotAgeValid(final long timestamp) {
        return timestamp + MINIMUMSNAPSHOTAGE < getCurrentTime();
    }

    @Override
    public boolean isTransactionTimestampValid(final long timestamp) {
        return timestamp >= getCurrentTime() - MAXTRANSACTIONAGE;
    }
    
    /**
     * Get the current timestamp.
     * @return the current timestamp
     */
    protected long getCurrentTime() {
        return CacheFactory.ensureCluster().getTimeMillis();
    }

}
