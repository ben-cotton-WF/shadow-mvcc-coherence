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

import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Constants for the domain package.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public final class Constants {

    /**
     * private constructor to prevent instantiation.
     */
    private Constants() {
    }

    /**
     * An instance of {@code PofExtractor} that may be used to obtain the logical key
     * from the {@link VersionedKey} in the version cache.
     */
    public static final PofExtractor LOGICALKEYEXTRACTOR = new PofExtractor(
            null, new SimplePofPath(VersionedKey.POF_LOGICALKEY), AbstractExtractor.KEY);
    /**
     * An instance of {@code PofExtractor} that may be used to obtain the {@code TransactionId}
     * from the {@link VersionedKey} in the version cache.
     */
    public static final PofExtractor TIMESTAMPEXTRACTOR = new PofExtractor(
            null, new SimplePofPath(VersionedKey.POF_TIMESTAMP), AbstractExtractor.KEY);
    /**
     * The decoration id used to store the commit status of a version cache entry.
     */
    public static final int DECO_COMMIT = ExternalizableHelper.DECO_APP_1;
    
    /**
     * The decoration id used to store the deleted status of a version cache entry.
     */
    public static final int DECO_DELETED = ExternalizableHelper.DECO_APP_2;

}
