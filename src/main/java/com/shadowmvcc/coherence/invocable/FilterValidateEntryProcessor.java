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

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;


/**
 * EntryProcessor to evaluate a filter against an entry.
 * This will return a {@link ProcessorResult} with the
 * logical key value if the filter matches, null otherwise
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical key type
 */
@Portable
public class FilterValidateEntryProcessor<K> extends AbstractMVCCProcessor<K, VersionedKey<K>> {

    private static final long serialVersionUID = -954213053828163546L;
    @PortableProperty(4)
    protected EntryProcessor delegate;

    /**
     * Default constructor for POF use only.
     */
    public FilterValidateEntryProcessor() {
        super();
    }

    /**
     * Constructor.
     * @param transactionId transaction id
     * @param isolationLevel isolation level
     * @param cacheName cache name
     * @param validationFilter filter to execute
     */
    public FilterValidateEntryProcessor(final TransactionId transactionId, 
            final IsolationLevel isolationLevel, final CacheName cacheName, 
            final Filter validationFilter) {
        super(transactionId, isolationLevel, cacheName, validationFilter);
    }


    @SuppressWarnings("unchecked")
    @Override
    public ProcessorResult<K, VersionedKey<K>> process(final Entry entryarg) {

        BinaryEntry entry = (BinaryEntry) entryarg;

        Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
        if (priorVersionBinaryKey == null) {
            return null;
        }

        ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, transactionId, isolationLevel, cacheName);

        try {
            if (!confirmFilterMatch(childEntry)) {
                return null;
            }
        } catch (AbstractEntryWrapper.ReadUncommittedException ex) {
            return new ProcessorResult<K, VersionedKey<K>>(ex.getCacheName(), (VersionedKey<K>) ex.getUncommittedKey());
        }

        BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry)
                .getBackingMapEntry(priorVersionBinaryKey);

        return new ProcessorResult<K, VersionedKey<K>>((VersionedKey<K>) priorEntry.getKey(), false, true);
    }

}
