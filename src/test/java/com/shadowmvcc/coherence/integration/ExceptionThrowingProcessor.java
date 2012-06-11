package com.shadowmvcc.coherence.integration;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * EntryProcessor that will either insert a value or wait a short while and throw an exception.
 * Used to test invokeAll failure scenario
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class ExceptionThrowingProcessor extends AbstractProcessor {
    
    private static final long serialVersionUID = 3419264759153656237L;
    
    @PortableProperty(0) private Object newValue;

    /**
     * Constructor.
     * @param newValue the new value
     */
    public ExceptionThrowingProcessor(final Object newValue) {
        super();
        this.newValue = newValue;
    }

    /**
     *  Default constructor for POF use only.
     */
    public ExceptionThrowingProcessor() {
        super();
    }

    @Override
    public Object process(final Entry entry) {
        
        Integer key = (Integer) entry.getKey();
        
        if (key == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            throw new RuntimeException("tried to update key 0");
        }
        
        Object result = entry.getValue();
        
        entry.setValue(newValue);
        
        return result;
    }

}
