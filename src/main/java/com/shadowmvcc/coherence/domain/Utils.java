/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

package com.shadowmvcc.coherence.domain;

import com.tangosol.io.Serializer;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;

/**
 * Utility class of static methods for manipulating domain classes.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public final class Utils {

    /**
     * private constructor to prevent instantiation.
     */
    private Utils() {
    }

    /**
     * Return the value of a decoration as a Binary.
     * @param decoId the decoration id
     * @param binEntry the BinaryEntry
     * @param defaultValue default value to return if the decoration is absent
     * @return the boolean value of the decoration
     */
    private static boolean getBooleanDecoration(
            final int decoId, final BinaryEntry binEntry, final boolean defaultValue) {
        boolean result = defaultValue;
        Binary binValue = binEntry.getBinaryValue();
        if (binValue != null) {
            if (ExternalizableHelper.isDecorated(binValue)) {
                Binary binDeco = ExternalizableHelper.getDecoration(binValue, decoId);
                if (binDeco != null) {
                    result = (Boolean) ExternalizableHelper.fromBinary(binDeco, binEntry.getSerializer());
                }
            }
        }

        return result;

    }

    /**
     * @param binaryEntry a version cache entry
     * @return true if the entry has been committed
     */
    public static boolean isCommitted(final BinaryEntry binaryEntry) {
        return getBooleanDecoration(Constants.DECO_COMMIT, binaryEntry, true);
    }

    /**
     * @param binaryEntry a version cache entry
     * @return true if the entry represents a deleted entry
     */
    public static boolean isDeleted(final BinaryEntry binaryEntry) {
        return getBooleanDecoration(Constants.DECO_DELETED, binaryEntry, false);
    }

    /**
     * Set the committed and deleted decorations on a version cache value.
     * @param binaryValue the binary version cache value
     * @param committed committed flag
     * @param deleted deleted flag
     * @param serializer the serializer to use
     * @return the decorated binary value
     */
    public static Binary decorateValue(final Binary binaryValue, final boolean committed, 
            final boolean deleted, final Serializer serializer) {

        Binary result = addBooleanDecoration(binaryValue, Constants.DECO_COMMIT, committed, serializer);
        result = addBooleanDecoration(result, Constants.DECO_DELETED, deleted, serializer);

        return result;

    }

    /**
     * Add a boolean value as decoration to a binary value.
     * @param binValue the value to decorate
     * @param decoId the decoration id
     * @param decoValue boolean value to decorate with
     * @param serializer the serializer to use
     * @return the decorated binary value
     */
    private static Binary addBooleanDecoration(final Binary binValue, final int decoId, 
            final boolean decoValue, final Serializer serializer) {
        Binary decoBin = ExternalizableHelper.toBinary(decoValue, serializer);
        return ExternalizableHelper.decorate(binValue, decoId, decoBin);
    }

    /**
     * Set the version cache entry as committed.
     * @param entry the version cache binary entry
     * @param committed value of committed flag
     */
    public static void setCommitted(final BinaryEntry entry, final boolean committed) {
        Binary binValue = entry.getBinaryValue();
        binValue = addBooleanDecoration(binValue, Constants.DECO_COMMIT, committed, entry.getSerializer());
        entry.updateBinaryValue(binValue);
    }

}
