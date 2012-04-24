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

package com.shadowmvcc.coherence.invocable;

import com.shadowmvcc.coherence.domain.Constants;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Extract and return a decoration value from a cache entry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class DecorationExtractorProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 5931592896780862265L;

    @PortableProperty(0)
    private int decoId;

    /**
     * Instance to extract the commit status {@code Boolean} value.
     */
    public static final DecorationExtractorProcessor COMMITTED_INSTANCE =
            new DecorationExtractorProcessor(Constants.DECO_COMMIT);
    /**
     * Instance to extract the deleted status {@code Boolean} value.
     */
    public static final DecorationExtractorProcessor DELETED_INSTANCE =
            new DecorationExtractorProcessor(Constants.DECO_DELETED);

    /**
     *  Default constructor for POF use only.
     */
    public DecorationExtractorProcessor() {
        super();
    }

    /**
     * @param decoId the decoration id to extract
     */
    public DecorationExtractorProcessor(final int decoId) {
        super();
        this.decoId = decoId;
    }

    @Override
    public Object process(final Entry entry) {
        BinaryEntry binEntry = (BinaryEntry) entry;
        Binary binValue = binEntry.getBinaryValue();
        if (ExternalizableHelper.isDecorated(binValue)) {
            Binary binDeco = ExternalizableHelper.getDecoration(binValue, decoId);
            if (binDeco != null) {
                return ExternalizableHelper.fromBinary(binDeco, binEntry.getSerializer());
            }
        }

        return null;
    }

}
