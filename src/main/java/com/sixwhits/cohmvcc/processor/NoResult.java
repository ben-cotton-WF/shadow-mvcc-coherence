package com.sixwhits.cohmvcc.processor;

/**
 * Special result type to indicate that no result should be returned from an EntryProcessor
 * invocation. The key will be omitted from the processAll result map
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public final class NoResult {
    
    /**
     * the singleton instance.
     */
    public static final NoResult INSTANCE = new NoResult();
    
    /**
     * Private constructor.
     */
    private NoResult() {
    }

}
