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
import com.shadowmvcc.coherence.domain.Utils;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.processor.NoResult;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Wrapper for a read only EntryProcessor. Invoke on the key cache
 * to operate on the version cache as if it were the logical cache.
 * 
 * Any update operation by the wrapped EntryProcessor will fail.
 * 
 * May optionally be provided with a filter. The {@code process}
 * method will return null if the filter does not match.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <R> EntryProcessor return type
 */
@Portable
public class MVCCReadOnlyEntryProcessorWrapper<K, R> extends AbstractMVCCProcessorWrapper<K, R> {

    private static final long serialVersionUID = -7158130705920331999L;

    /**
     *  Default constructor for POF use only.
     */
    public MVCCReadOnlyEntryProcessorWrapper() {
        super();
    }

    /**
     * Constructor.
     * @param transactionId transaction id
     * @param delegate EntryProcesor to execute
     * @param isolationLevel isolation level
     * @param cacheName cache name
     */
    public MVCCReadOnlyEntryProcessorWrapper(final TransactionId transactionId, 
            final EntryProcessor delegate, final IsolationLevel isolationLevel, final CacheName cacheName) {
        super(transactionId, isolationLevel, cacheName, delegate);
    }

    /**
     * Constructor with filter.
     * @param transactionId transaction id
     * @param delegate EntryProcesor to execute
     * @param isolationLevel isolation level
     * @param cacheName cache name
     * @param filter validation filter
     */
    public MVCCReadOnlyEntryProcessorWrapper(final TransactionId transactionId, 
            final EntryProcessor delegate, final IsolationLevel isolationLevel, final CacheName cacheName,
            final Filter filter) {
        super(transactionId, isolationLevel, cacheName, filter, delegate);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProcessorResult<K, R> process(final Entry entryarg) {

        BinaryEntry entry = (BinaryEntry) entryarg;
        Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);
        if (priorVersionBinaryKey == null) {
            return null;
        }

        BinaryEntry priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry)
                .getBackingMapEntry(priorVersionBinaryKey);

        if (isolationLevel != IsolationLevel.readUncommitted) {
            boolean committed = Utils.isCommitted(priorEntry);
            if (!committed) {
                return new ProcessorResult<K, R>(cacheName, (VersionedKey<K>) priorEntry.getKey());
            }
        }

        boolean deleted = Utils.isDeleted(priorEntry);
        if (deleted) {
            return null;
        }

        R result = null;

        if (delegate != null) {

            ReadOnlyEntryWrapper childEntry = new ReadOnlyEntryWrapper(entry, transactionId, isolationLevel, cacheName);

            if (!confirmFilterMatch(childEntry)) {
                return null;
            }

            try {
                result = (R) delegate.process(childEntry);
            } catch (AbstractEntryWrapper.ReadUncommittedException ex) {
                // Shouldn't happen as we've already explicitly checked before calling
                return new ProcessorResult<K, R>(ex.getCacheName(), (VersionedKey<K>) ex.getUncommittedKey());
            }

        }

        if ((isolationLevel == IsolationLevel.repeatableRead || isolationLevel == IsolationLevel.serializable)) {
            setReadTimestamp(entry);
        }

        if (result == NoResult.INSTANCE) {
            return new ProcessorResult<K, R>(null, null, false);
        } else {
            return new ProcessorResult<K, R>(result, null, true);
        }
    }

}
