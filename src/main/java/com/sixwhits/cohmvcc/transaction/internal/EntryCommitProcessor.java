package com.sixwhits.cohmvcc.transaction.internal;

import com.sixwhits.cohmvcc.domain.Utils;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Set a version cache entry to committed.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class EntryCommitProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 2004629159766780786L;

    public static final EntryCommitProcessor INSTANCE = new EntryCommitProcessor();

    @Override
    public Object process(final Entry arg) {
        BinaryEntry entry = (BinaryEntry) arg;
        Utils.setCommitted(entry, true);
        return null;
    }
}
