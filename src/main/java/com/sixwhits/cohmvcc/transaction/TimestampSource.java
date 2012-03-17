package com.sixwhits.cohmvcc.transaction;

/**
 * Define a source of timestamps. Implementations must guarantee that successive
 * calls will return values no less than provided by any previous call.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface TimestampSource {

    /**
     * @return a new timestamp
     */
    long getTimestamp();

}
