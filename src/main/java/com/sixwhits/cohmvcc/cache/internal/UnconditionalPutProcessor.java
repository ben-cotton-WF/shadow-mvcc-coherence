package com.sixwhits.cohmvcc.cache.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * {@code EntryProcessor} to put a value unconditionally.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class UnconditionalPutProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 1660410324368769032L;
    public static final int POF_RETURNPRIOR = 0;
    @PortableProperty(POF_RETURNPRIOR)
    private boolean returnPrior;
    public static final int POF_VALUE = 1;
    @PortableProperty(POF_VALUE)
    private Object value;

    /**
     * Default constructor for POF use only.
     */
    public UnconditionalPutProcessor() {
        super();
    }

    /**
     * @param value new value
     * @param returnPrior return the old value if true, otherwise null
     */
    public UnconditionalPutProcessor(final Object value, final boolean returnPrior) {
        super();
        this.returnPrior = returnPrior;
        this.value = value;
    }

    @Override
    public Object process(final Entry entry) {
        //TODO Need to replace this to not deserialise value
        Object result = returnPrior ? entry.getValue() : null;
        entry.setValue(value, false);
        return result;
    }

}
