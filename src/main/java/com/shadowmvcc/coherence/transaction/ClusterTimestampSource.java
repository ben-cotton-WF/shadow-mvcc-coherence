package com.shadowmvcc.coherence.transaction;

import com.tangosol.net.CacheFactory;

/**
 * Obtain timestamps using Coherence cluster time.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ClusterTimestampSource implements TimestampSource {

    @Override
    public long getTimestamp() {
        return CacheFactory.getCluster().getTimeMillis();
    }

}
