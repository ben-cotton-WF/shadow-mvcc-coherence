package com.sixwhits.cohmvcc.invocable;

import com.tangosol.net.InvocationObserver;

public interface InvocationObserverStatus extends InvocationObserver {
    
    boolean isFailed();
    

}
