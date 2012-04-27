package com.shadowmvcc.coherence.config;

import com.tangosol.net.CacheFactory;

/**
 * Standard cluster time provider.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ClusterTimeProviderImpl implements ClusterTimeProvider {

    @Override
    public long getClusterTime() {
        return CacheFactory.getCluster().getTimeMillis();
    }

}
