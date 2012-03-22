package com.sixwhits.cohmvcc.transaction.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Roll back an uncommitted version cache entry by removing it.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class EntryRollbackProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 3573370467378537711L;

    /**
     * An instance of {@code EntryRollbackProcessor}.
     */
    public static final EntryRollbackProcessor INSTANCE = new EntryRollbackProcessor();

    @Override
    public Object process(final Entry entry) {
        entry.remove(false);
        return null;
    }
}
