package com.sixwhits.cohmvcc.invocable;

import com.tangosol.net.InvocationObserver;

/**
 * Extension for an {@code InvocationObserver} that records its own
 * success or failure state.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface InvocationObserverStatus extends InvocationObserver {
    
    /**
     * Has the observed invocation failed.
     * @return true if the invocation has failed
     */
    boolean isFailed();
    

}
