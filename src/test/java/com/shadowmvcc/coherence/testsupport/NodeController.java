package com.shadowmvcc.coherence.testsupport;

import com.shadowmvcc.coherence.monitor.CacheFactoryBuilder;
import com.tangosol.net.CacheFactory;

/**
 * Utility class to stop a node's monitor thread gracefully under littlegrid.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public final class NodeController {
    
    /**
     * Private constructor to prevent instantiation.
     */
    public NodeController() {
    }
    
    /**
     * Stop the cluster.
     */
    public void stop() {
        ((CacheFactoryBuilder) CacheFactory.getCacheFactoryBuilder()).stopMonitorThread();
        CacheFactory.getCluster().stop();
    }

}
