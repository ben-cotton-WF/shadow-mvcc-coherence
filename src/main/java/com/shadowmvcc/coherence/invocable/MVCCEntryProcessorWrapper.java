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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.DeletedObject;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.ProcessorResult;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.Utils;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.exception.FutureReadException;
import com.shadowmvcc.coherence.invocable.MVCCBackingMapManagerContext.MVCCBackingMapContext;
import com.shadowmvcc.coherence.processor.NoResult;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.BackingMapManagerContext;
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
        
        Set<CacheName> referencedCacheNames = new HashSet<CacheName>();
        if (delegate instanceof MultiCacheProcessor) {
            referencedCacheNames.addAll(((MultiCacheProcessor) delegate).getReferencedMVCCCacheNames());
        }
        referencedCacheNames.add(cacheName);
        
        final BackingMapManagerContext parentContext = entry.getBackingMapContext().getManagerContext();
        
        MVCCBackingMapManagerContext bmcc =
                new MVCCBackingMapManagerContext(parentContext, referencedCacheNames, transactionId, isolationLevel);

        ReadWriteEntryWrapper childEntry = (ReadWriteEntryWrapper) bmcc.getBackingMapContext(
                cacheName.getLogicalName()).getBackingMapEntry(entry.getBinaryKey());

        if (!confirmFilterMatch(childEntry)) {
            return null;
        }

        R result;
        try {
            result = (R) delegate.process(childEntry);
        } catch (AbstractEntryWrapper.ReadUncommittedException ex) {
            return new ProcessorResult<K, R>(ex.getCacheName(), (VersionedKey<K>) ex.getUncommittedKey());
        }
        
        for (MVCCBackingMapContext bmc : bmcc.getMVCCBackingMapContexts()) {
            if (bmc != null) {
                for (ReadWriteEntryWrapper rwew : bmc.getWrappedEntries()) {
                    validateChildEntry(rwew);
                }
            }
        }
        
        Map<CacheName, Set<Object>> changedKeys = null;

        for (MVCCBackingMapContext bmc : bmcc.getMVCCBackingMapContexts()) {
            if (bmc != null) {
                for (ReadWriteEntryWrapper rwew : bmc.getWrappedEntries()) {
                    changedKeys = effectCacheChange(rwew, changedKeys);
                }
            }
        }
        
        if (result == NoResult.INSTANCE) {
            return new ProcessorResult<K, R>(null, changedKeys, false);
        } else {
            return new ProcessorResult<K, R>(result, changedKeys, true);
        }
    }
    
    /**
     * Check that the result of processing doesn't break any transaction constraints.
     * @param childEntry the version cache entry
     */
    @SuppressWarnings("unchecked")
    private void validateChildEntry(final ReadWriteEntryWrapper childEntry) {
        
        if (childEntry.isPriorRead() && isolationLevel == IsolationLevel.readProhibited) {
            throw new IllegalStateException("Read of prior version with isolation level readProhibited: "
                    + childEntry.getKey());
        }

        if (isModified(childEntry)) {

            TransactionId nextRead = getNextRead(childEntry.getParentEntry());
            if (nextRead != null) {
                TransactionId nextWrite = getNextWrite(childEntry);
                if (nextWrite == null || nextRead.compareTo(nextWrite) <= 0) {
                    //TODO add the cache name
                    throw new FutureReadException(new VersionedKey<K>((K) childEntry.getKey(), nextRead));
                }
            }
        }
    }
    
    /**
     * Perform the changes to the version cache and record the keys that have changed.
     * @param childEntry the wrapped entry
     * @param changedKeysIn the map of changed keys, which may be null
     * @return the map of changed keys, which may change from that passed
     */
    private Map<CacheName, Set<Object>> effectCacheChange(final ReadWriteEntryWrapper childEntry,
            final Map<CacheName, Set<Object>> changedKeysIn) {
        
        Map<CacheName, Set<Object>> changedKeys = changedKeysIn;
        
        if (isModified(childEntry)) {

            BinaryEntry newEntry = childEntry.getVersionCacheEntry();

            Binary binaryValue;

            if (childEntry.isRemove()) {
                binaryValue = (Binary) childEntry.getContext()
                        .getValueToInternalConverter().convert(DeletedObject.INSTANCE);
            } else {
                binaryValue = childEntry.getNewBinaryValue();
            }

            binaryValue = Utils.decorateValue(
                    binaryValue, autoCommit, childEntry.isRemove(), childEntry.getSerializer());

            newEntry.updateBinaryValue(binaryValue);
            
            if (changedKeys == null) {
                changedKeys = new HashMap<CacheName, Set<Object>>();
            }
            CacheName cacheName = childEntry.getCacheName();
            if (!changedKeys.containsKey(cacheName)) {
                changedKeys.put(cacheName, new HashSet<Object>());
            }
            changedKeys.get(cacheName).add(childEntry.getKey());
        }

        if ((isolationLevel == IsolationLevel.repeatableRead
                || isolationLevel == IsolationLevel.serializable) && childEntry.isPriorRead()) {
            setReadTimestamp(childEntry.getParentEntry());
        }
        
        return changedKeys;
        
    }
    
    /**
     * Determine if a wrapped entry has been modified.
     * @param childEntry the wrapped entry to check
     * @return true if modified
     */
    private boolean isModified(final ReadWriteEntryWrapper childEntry) {
        return childEntry.isRemove() || childEntry.getNewBinaryValue() != null;
    }
}
