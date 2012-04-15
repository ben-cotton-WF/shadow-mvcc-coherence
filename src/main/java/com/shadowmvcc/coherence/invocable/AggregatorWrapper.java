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
