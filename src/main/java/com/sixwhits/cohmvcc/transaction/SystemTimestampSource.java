package com.sixwhits.cohmvcc.transaction;


/**
 * Generate timestamp using the local system time.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SystemTimestampSource implements TimestampSource {

    @Override
    public long getTimestamp() {
        return System.currentTimeMillis();
    }

}
