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

package com.shadowmvcc.coherence.index;

import java.util.Map.Entry;

import com.shadowmvcc.coherence.invocable.VersionCacheBinaryEntryWrapper;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.EntryFilter;

/**
 * Wrap a filter so that it can be given an entry that looks like
 * the logical cache view.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class FilterWrapper implements EntryFilter {

    protected final Filter delegate;

    /**
     * Constructor.
     * @param delegate the filter to wrap
     */
    public FilterWrapper(final Filter delegate) {
        super();
        this.delegate = delegate;
    }

    @Override
    public boolean evaluate(final Object obj) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean evaluateEntry(@SuppressWarnings("rawtypes") final Entry arg) {
        BinaryEntry entry = (BinaryEntry) arg;
        BinaryEntry wrappedEntry = new VersionCacheBinaryEntryWrapper(entry);
        if (delegate instanceof EntryFilter) {
            return ((EntryFilter) delegate).evaluateEntry(wrappedEntry);
        } else {
            return delegate.evaluate(wrappedEntry.getValue());
        }
    }

}
