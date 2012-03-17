package com.sixwhits.cohmvcc.invocable;

import com.tangosol.util.BinaryEntry;

/**
 * Interface extending {@link BinaryEntry} to allow {@code EntryProcessor}
 * wrappers to obtain the information needed.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface EntryWrapper extends BinaryEntry {

    /**
     * @return true if after processing, this entry is to be removed
     */
    boolean isRemove();

}