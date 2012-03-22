package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.annotation.Portable;

/**
 * Stub class to represent a deleted object. Required as it is not possible
 * to decorate a null cache value in Coherence.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class DeletedObject {

    /**
     * An instance of DeletedObject.
     */
    public static final DeletedObject INSTANCE = new DeletedObject();

    @Override
    public int hashCode() {
        return 31;
    }
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return true;
    }

}
