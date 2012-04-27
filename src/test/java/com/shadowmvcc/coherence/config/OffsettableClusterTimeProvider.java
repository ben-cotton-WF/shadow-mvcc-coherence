package com.shadowmvcc.coherence.config;

/**
 * Test cluster time provider that we can apply an offset to to make time jump.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class OffsettableClusterTimeProvider implements ClusterTimeProvider {

    private long offset = 0L;
    private ClusterTimeProvider delegate = new ClusterTimeProviderImpl();
    
    @Override
    public long getClusterTime() {
        return delegate.getClusterTime() + offset;
    }
    
    /**
     * Change the real time offset.
     * @param offset the offset to set
     */
    public void setOffset(final long offset) {
        this.offset = offset;
    }

}
