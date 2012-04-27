package com.shadowmvcc.coherence.config;

/**
 * Interface for obtaining the cluster time. Abstracted so we can override for testing.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface ClusterTimeProvider {
    /**
     * Get the cluster time.
     * @return the time in milliseconds
     */
    long getClusterTime();
}
