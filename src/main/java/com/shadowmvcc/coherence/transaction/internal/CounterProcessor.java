package com.shadowmvcc.coherence.transaction.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Get a value from a counter. Simply increments and returns a counter
 * starting from zero if the entry does not exist
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class CounterProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 44873576034046440L;
    public static final CounterProcessor INSTANCE = new CounterProcessor();
    
    @Override
    public Object process(final Entry entry) {
        Integer managerId;
        
        if (!entry.isPresent()) {
            managerId = 0;
        } else {
            managerId = (Integer) entry.getValue();
        }
        managerId++;
        entry.setValue(managerId);
        
        return managerId;
    }

}
