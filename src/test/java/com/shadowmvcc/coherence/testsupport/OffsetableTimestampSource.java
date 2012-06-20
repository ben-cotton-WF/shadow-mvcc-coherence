package com.shadowmvcc.coherence.testsupport;

import com.shadowmvcc.coherence.transaction.TimestampSource;

/**
 * Simple timestamp source that delegates to another source and applies an offset.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class OffsetableTimestampSource implements TimestampSource {

    private final TimestampSource delegate;
    private long offset = 0;

    /**
     * Constructor.
     * @param delegate source of real timestamps
     */
    public OffsetableTimestampSource(final TimestampSource delegate) {
        super();
        this.delegate = delegate;
    }
    
    @Override
    public long getTimestamp() {
        return delegate.getTimestamp() + offset;
    }

    /**
     * Set the offset to the real timestamps.
     * @param offset time offset to apply
     */
    public void setOffset(final long offset) {
        this.offset = offset;
    }

}
