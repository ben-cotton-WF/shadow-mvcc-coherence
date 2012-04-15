/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

package com.shadowmvcc.coherence.invocable;

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
