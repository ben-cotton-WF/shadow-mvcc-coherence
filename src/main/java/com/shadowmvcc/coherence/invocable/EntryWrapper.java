package com.shadowmvcc.coherence.invocable;

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
     * Has this entry been marked for removal by the {@code EntryProcessor}?
     * @return true if after processing, this entry is to be removed
     */
    boolean isRemove();

}