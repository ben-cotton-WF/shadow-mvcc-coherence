package com.shadowmvcc.coherence.transaction.internal;

import com.shadowmvcc.coherence.config.ClusterTimeProviderFactory;
import com.shadowmvcc.coherence.config.ConfigurationFactory;

/**
 * Implementation of timestamp validator.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class TimestampValidatorImpl implements TimestampValidator {
    
    @Override
    public boolean isSnapshotAgeValid(final long timestamp) {
        long minimumSnapshotAge = ConfigurationFactory.getConfiguraration().getMinimumSnapshotAge();
        long currentTime = getCurrentTime();
        return timestamp + minimumSnapshotAge < currentTime;
    }

    @Override
    public boolean isTransactionTimestampValid(final long timestamp) {
        return timestamp >= getCurrentTime() - ConfigurationFactory.getConfiguraration().getMaximumTransactionAge();
    }
    
    /**
     * Get the current timestamp.
     * @return the current timestamp
     */
    protected long getCurrentTime() {
        return ClusterTimeProviderFactory.getInstance().getClusterTime();
    }

}
