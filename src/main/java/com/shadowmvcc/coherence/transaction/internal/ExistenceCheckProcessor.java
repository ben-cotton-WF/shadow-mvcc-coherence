package com.shadowmvcc.coherence.transaction.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Check whether a version cache entry is present.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class ExistenceCheckProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 6193489100316487489L;

    @Override
    public Object process(final Entry entry) {
        return entry.isPresent();
    }

}
