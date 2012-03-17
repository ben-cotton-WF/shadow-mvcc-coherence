package com.sixwhits.cohmvcc.cache.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * {@code EntryProcessor} to remove a cache entry.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class UnconditionalRemoveProcessor extends AbstractProcessor {

    private static final long serialVersionUID = -1589869423220481276L;
    public static final int POF_RETURN = 0;
    @PortableProperty(POF_RETURN)
    private boolean returnPrior = true;

    /**
     * Default constructor for POF use only.
     */
    public UnconditionalRemoveProcessor() {
        super();
    }

    /**
     * @param returnPrior return the old value if true, else null
     */
    public UnconditionalRemoveProcessor(final boolean returnPrior) {
        super();
        this.returnPrior = returnPrior;
    }

    @Override
    public Object process(final Entry entry) {
        entry.remove(false);
        return returnPrior ? entry.getValue() : null;
    }
}
