package com.sixwhits.cohmvcc.invocable;

import java.util.SortedSet;

import com.sixwhits.cohmvcc.pof.SortedSetCodec;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Append an item to the end of a sorted set value object.
 * Returns true if successful, false if the item
 * already exists.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <T> type of the entry being added and of the set
 */
@Portable
public class SortedSetAppender<T extends Comparable<T>> extends AbstractProcessor {

    private static final long serialVersionUID = 8348665851594399438L;

    @PortableProperty (value = 0, codec = SortedSetCodec.class) private SortedSet<T> defaultInitialSet;
    @PortableProperty (1) private T value;
    
    /**
     *  Default constructor for POF use only.
     */
    public SortedSetAppender() {
        super();
    }

    /**
     * Constructor.
     * @param defaultInitialSet initial set if not already present.
     * @param value value to add to the end of the set
     */
    public SortedSetAppender(final SortedSet<T> defaultInitialSet, final T value) {
        super();
        this.defaultInitialSet = defaultInitialSet;
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object process(final Entry entry) {
        
        SortedSet<T> theSet;
        
        if (entry.isPresent()) {
            theSet = (SortedSet<T>) entry.getValue();
        } else {
            theSet = defaultInitialSet;
        }
        
        if (theSet.contains(value)) {
            return false;
        }
        
        if (theSet.last().compareTo(value) > 0) {
            return false;
        }
        
        theSet.add(value);
        
        entry.setValue(theSet);
        
        return true;
        
    }

}
