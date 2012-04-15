package com.shadowmvcc.coherence.invocable;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Dummy EntryProcessor for testing.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class DummyBinaryProcessor extends AbstractProcessor {

    private static final long serialVersionUID = -5678432703915042092L;

    @Override
    public Object process(final Entry arg0) {
        BinaryEntry entry = (BinaryEntry) arg0;

        Binary binLogicalVal2 = entry.getBinaryValue();

        Object result = entry.getBackingMapContext()
                .getManagerContext().getValueFromInternalConverter().convert(binLogicalVal2);

        return result;
    }

}
