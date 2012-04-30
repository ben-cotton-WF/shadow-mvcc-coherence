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

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.Utils;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.index.MVCCExtractor;
import com.shadowmvcc.coherence.index.MVCCIndex;
import com.tangosol.io.Serializer;
import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ObservableMap;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Abstract base class for entry wrappers that translate the physical cache
 * view where the previous value is a separate cache entry, to the logical view
 * allowing access to that previous value.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public abstract class AbstractEntryWrapper implements EntryWrapper {

    private final BinaryEntry parentEntry;
    private BinaryEntry priorBinaryEntry = null;
    private final TransactionId transactionId;
    private final IsolationLevel isolationLevel;
    private boolean priorRead = false;
    private CacheName cacheName;
    private final Collection<CacheName> mvccCacheNames;
    
    /**
     * Exception to throw on finding an uncommitted read.
     * 
     * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
     *
     */
    public class ReadUncommittedException extends RuntimeException {
        private final CacheName cacheName;
        private final VersionedKey<?> uncommittedKey;
        private static final long serialVersionUID = 1L;
        
        /**
         * Constructor.
         * @param cacheName name of the cache containing uncommitted entry
         * @param uncommittedKey version cache key of the uncommitted entry
         */
        public ReadUncommittedException(final CacheName cacheName, final VersionedKey<?> uncommittedKey) {
            super();
            this.cacheName = cacheName;
            this.uncommittedKey = uncommittedKey;
        }

        /**
         * Get the key of the uncommitted entry that gave rise to this exception.
         * @return the key of the uncommitted entry
         */
        public VersionedKey<?> getUncommittedKey() {
            return uncommittedKey;
        }

        /**
         * Get the name of the cache in which the uncommitted entry was found.
         * @return the name of the cache
         */
        public CacheName getCacheName() {
            return cacheName;
        }
        
    }

    /**
     * Constructor.
     * @param parentEntry parent BinaryEntry from the key cache
     * @param transactionId transaction id of the enclosing transaction
     * @param isolationLevel isolation level of the enclosing transaction
     * @param cacheName name of the current cache
     */
    public AbstractEntryWrapper(final BinaryEntry parentEntry, final TransactionId transactionId,
            final IsolationLevel isolationLevel, final CacheName cacheName) {
        super();
        this.parentEntry = parentEntry;
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.cacheName = cacheName;
        this.mvccCacheNames = null;
    }
    
    /**
     * Constructor.
     * @param parentEntry parent BinaryEntry from the key cache
     * @param transactionId transaction id of the enclosing transaction
     * @param isolationLevel isolation level of the enclosing transaction
     * @param cacheName name of the current cache
     * @param mvccCacheNames collection of other MVCC cache names that may be referenced
     */
    public AbstractEntryWrapper(final BinaryEntry parentEntry, final TransactionId transactionId,
            final IsolationLevel isolationLevel, final CacheName cacheName,
            final Collection<CacheName> mvccCacheNames) {
        super();
        this.parentEntry = parentEntry;
        this.transactionId = transactionId;
        this.isolationLevel = isolationLevel;
        this.cacheName = cacheName;
        this.mvccCacheNames = null;
    }

    /**
     * @return the backing map context for the current caches
     */
    private BackingMapContext getVersionCacheBackingMapContext() {
        return parentEntry.getBackingMapContext().getManagerContext().getBackingMapContext(
                cacheName.getVersionCacheName());
    }

    @Override
    public Object extract(final ValueExtractor valueextractor) {
        if (valueextractor instanceof PofExtractor) {
            return ((PofExtractor) valueextractor).extractFromEntry(this);
        } else {
            return valueextractor.extract(getValue());
        }
    }

    @Override
    public Object getKey() {
        return parentEntry.getKey();
    }

    /**
     * @param <K> logical key type
     * @return the binary entry from the version cache for the previous version
     */
    private <K> BinaryEntry getPriorBinaryEntry() {
        if (!priorRead) {
            @SuppressWarnings("unchecked")
            MVCCIndex<K> index = (MVCCIndex<K>) getVersionCacheBackingMapContext()
                    .getIndexMap().get(MVCCExtractor.INSTANCE);
            @SuppressWarnings("unchecked")
            Binary priorVersionBinaryKey = index.floor((K) parentEntry.getKey(), transactionId);

            if (priorVersionBinaryKey != null) {

                priorBinaryEntry = (BinaryEntry) getVersionCacheBackingMapContext()
                        .getBackingMapEntry(priorVersionBinaryKey);
                
                if (isolationLevel != IsolationLevel.readUncommitted
                        && isolationLevel != IsolationLevel.readProhibited) {

                    if (isolationLevel != IsolationLevel.readUncommitted) {
                        boolean committed = Utils.isCommitted(priorBinaryEntry);
                        if (!committed) {
                            throw new ReadUncommittedException(cacheName, (VersionedKey<?>) priorBinaryEntry.getKey());
                        }
                    }
                }
            }
            
        }
        priorRead = true;
        return priorBinaryEntry;
    }

    @Override
    public Binary getOriginalBinaryValue() {

        BinaryEntry priorEntry = getPriorBinaryEntry();
        Binary result = null;

        if (priorEntry != null) {
            result = priorEntry.getOriginalBinaryValue();
        }

        return result;
    }

    @Override
    public boolean isPresent() {
        BinaryEntry priorEntry = getPriorBinaryEntry();
        return (priorEntry != null);
    }

    @Override
    public Binary getBinaryKey() {
        return parentEntry.getBinaryKey();
    }

    @Override
    public Serializer getSerializer() {
        return parentEntry.getSerializer();
    }

    @Override
    public BackingMapManagerContext getContext() {
        return getBackingMapContext().getManagerContext();
    }

    @Override
    public Object getOriginalValue() {
        return getContext().getValueFromInternalConverter().convert(getOriginalBinaryValue());
    }

    @Override
    public ObservableMap getBackingMap() {
        return getBackingMapContext().getBackingMap();
    }

    @Override
    public BackingMapContext getBackingMapContext() {
        return getVersionCacheBackingMapContext();
    }

    @Override
    public void expire(final long l) {
        throw new UnsupportedOperationException("expiry of MVCC cache entries not supported");
    }

    /**
     * @return true if the previous version has been read
     */
    public boolean isPriorRead() {
        return priorRead;
    }

}