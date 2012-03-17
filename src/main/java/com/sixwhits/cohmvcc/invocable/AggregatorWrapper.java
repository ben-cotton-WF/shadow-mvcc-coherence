package com.sixwhits.cohmvcc.invocable;

import java.util.Set;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.EntryAggregator;

/**
 * Wrapper for an {@code EntryAggregator} that invokes the
 * wrapped aggregator with a wrapped set of version cache entries
 * so that the wrapped aggregator sees them as a set of logical 
 * cache entries.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class AggregatorWrapper implements EntryAggregator {

    private static final long serialVersionUID = 7022940621318350536L;

    public static final int POF_DELEGATE = 0;
    @PortableProperty(POF_DELEGATE)
    private EntryAggregator delegate;

    /**
     *  Default constructor for POF use only.
     */
    public AggregatorWrapper() {
        super();
    }

    /**
     * @param delegate the wrapped aggregator
     */
    public AggregatorWrapper(final EntryAggregator delegate) {
        super();
        this.delegate = delegate;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object aggregate(final Set set) {
        return delegate.aggregate(new VersionWrapperSet(set));
    }

}
