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
import com.shadowmvcc.coherence.domain.DeletedObject;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.Utils;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.exception.FutureReadException;
import com.shadowmvcc.coherence.processor.NoResult;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Wrapper class to execute an {@code EntryProcessor} written from the logical cache
 * perspective, against the key cache.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> Key type
 * @param <R> Return value type
 */
@Portable
public class MVCCEntryProcessorWrapper<K, R> extends AbstractMVCCProcessorWrapper<K, R> {

    private static final long serialVersionUID = -7158130705920331999L;

    @PortableProperty(11) private boolean autoCommit = false;

    /**
     * Default constructor for POF use only.
     */
    public MVCCEntryProcessorWrapper() {
    }

    /**
     * Constructor.
     * @param transactionId current transaction id
     * @param delegate EntryProcessor to execute
     * @param isolationLevel current isolation level
     * @param autoCommit implicit commit if true
     * @param cacheName cache name
     */
    public MVCCEntryProcessorWrapper(final TransactionId transactionId, 
            final EntryProcessor delegate, final IsolationLevel isolationLevel,
            final boolean autoCommit, final CacheName cacheName) {
        super(transactionId, isolationLevel, cacheName, delegate);
        this.autoCommit = autoCommit;
    }

    /**
     * Constructor.
     * @param transactionId current transaction id
     * @param delegate EntryProcessor to execute
     * @param isolationLevel current isolation level
     * @param autoCommit implicit commit if true
     * @param cacheName cache name
     * @param filter filter to confirm whether to process
     */
    public MVCCEntryProcessorWrapper(final TransactionId transactionId, 
            final EntryProcessor delegate, final IsolationLevel isolationLevel,
            final boolean autoCommit, final CacheName cacheName, final Filter filter) {
        super(transactionId, isolationLevel, cacheName, filter, delegate);
        this.autoCommit = autoCommit;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProcessorResult<K, R> process(final Entry entryarg) {

        BinaryEntry entry = (BinaryEntry) entryarg;
        BinaryEntry priorEntry = null;
        Binary priorVersionBinaryKey = getPriorVersionBinaryKey(entry);

        if (priorVersionBinaryKey != null) {

            priorEntry = (BinaryEntry) getVersionCacheBackingMapContext(entry)
                    .getBackingMapEntry(priorVersionBinaryKey);
            
            if (isolationLevel != IsolationLevel.readUncommitted && isolationLevel != IsolationLevel.readProhibited) {

                if (isolationLevel != IsolationLevel.readUncommitted) {
                    boolean committed = Utils.isCommitted(priorEntry);
                    if (!committed) {
                        return new ProcessorResult<K, R>((VersionedKey<K>) priorEntry.getKey());
                    }
                }
            }
        }

        ReadWriteEntryWrapper childEntry = new ReadWriteEntryWrapper(entry, priorEntry, cacheName);

        if (!confirmFilterMatch(childEntry)) {
            return null;
        }

        R result = (R) delegate.process(childEntry);

        if (childEntry.isPriorRead() && isolationLevel == IsolationLevel.readProhibited) {
            throw new IllegalStateException("Read of prior version with isolation level readProhibited: "
                    + entry.getKey());
        }

        boolean changed = false;
        if (childEntry.isRemove() || childEntry.getNewBinaryValue() != null) {
            changed = true;

            TransactionId nextRead = getNextRead(entry);
            if (nextRead != null) {
                TransactionId nextWrite = getNextWrite(childEntry);
                if (nextWrite == null || nextRead.compareTo(nextWrite) <= 0) {
                    throw new FutureReadException(new VersionedKey<K>((K) childEntry.getKey(), nextRead));
                }
            }

            Binary binaryKey = (Binary) childEntry.getContext().getKeyToInternalConverter().convert(
                    new VersionedKey<K>((K) childEntry.getKey(), transactionId));
            BinaryEntry newEntry = (BinaryEntry) childEntry.getBackingMapContext().getBackingMapEntry(binaryKey);

            Binary binaryValue;

            if (childEntry.isRemove()) {
                binaryValue = (Binary) childEntry.getContext()
                        .getValueToInternalConverter().convert(DeletedObject.INSTANCE);
            } else {
                binaryValue = childEntry.getNewBinaryValue();
            }

            binaryValue = Utils.decorateValue(binaryValue, autoCommit, childEntry.isRemove(), entry.getSerializer());

            newEntry.updateBinaryValue(binaryValue);
        }

        if ((isolationLevel == IsolationLevel.repeatableRead
                || isolationLevel == IsolationLevel.serializable) && childEntry.isPriorRead()) {
            setReadTimestamp(entry);
        }
        
        if (result == NoResult.INSTANCE) {
            return new ProcessorResult<K, R>(null, changed, false);
        } else {
            return new ProcessorResult<K, R>(result, changed, true);
        }
    }

}
