package com.sixwhits.cohmvcc.pof;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.reflect.PofNavigator;
import com.tangosol.io.pof.reflect.PofValue;

/**
 * Navigator that returns the object itself.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class IdentityPofPath implements PofNavigator {

    public static final IdentityPofPath INSTANCE = new IdentityPofPath();

    @Override
    public PofValue navigate(final PofValue pofvalue) {
        return pofvalue;
    }


}
