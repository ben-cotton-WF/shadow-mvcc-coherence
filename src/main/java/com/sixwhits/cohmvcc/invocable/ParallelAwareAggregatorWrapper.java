package com.sixwhits.cohmvcc.invocable;

import java.util.Collection;
import java.util.Set;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.ParallelAwareAggregator;

/**
 * Wraps a {@code ParallelAwareAggregator} so that we can aggregate over
 * a set of version cache entries as if they were logical cache entries.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class ParallelAwareAggregatorWrapper implements ParallelAwareAggregator {

    private static final long serialVersionUID = -1892144268045361220L;

    public static final int POF_DELEGATE = 0;
    @PortableProperty(POF_DELEGATE)
    private ParallelAwareAggregator delegate;

    /**
     * Default constructor for POF use only.
     */
    public ParallelAwareAggregatorWrapper() {
        super();
    }

    /**
     * Constructor.
     * @param delegate the aggregator to wrap
     */
    public ParallelAwareAggregatorWrapper(final ParallelAwareAggregator delegate) {
        super();
        this.delegate = delegate;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Object aggregate(final Set set) {
        return delegate.aggregate(new VersionWrapperSet(set));
    }

    @Override
    public EntryAggregator getParallelAggregator() {
        return new ParallelAwareAggregatorWrapper((ParallelAwareAggregator) delegate.getParallelAggregator());
    }

    @Override
    public Object aggregateResults(@SuppressWarnings("rawtypes") final Collection collection) {
        return delegate.aggregateResults(collection);
    }

}
