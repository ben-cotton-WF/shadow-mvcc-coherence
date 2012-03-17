package com.sixwhits.cohmvcc.index;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sixwhits.cohmvcc.domain.Constants;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.Utils;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Converter;
import com.tangosol.util.MapIndex;
import com.tangosol.util.ValueExtractor;

/**
 * Custom index for the version cache. Maintains an ordered map of transaction ids for each logical key and provides
 * methods to find the next, previous etc entries relative to a specified starting timestamp.
 * 
 * @author David Whitmarsh, based on an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 * 
 * @param <K> the key type
 */
public class MVCCIndex<K> implements MapIndex {
    
    /**
     * Entry class for the index. Keep the binary key of the entry to link to
     * together with its committed and deleted states for efficient traversal.
     * 
     * Only the committed status of an entry is mutable.
     * 
     */
    public static class IndexEntry {
        private boolean isCommitted;
        private final boolean isDeleted;
        private final Binary binaryKey;
        /**
         * @param isCommitted committed flag
         * @param isDeleted deleted flag
         * @param binaryKey version cache key
         */
        public IndexEntry(final boolean isCommitted, final boolean isDeleted, final Binary binaryKey) {
            super();
            this.isCommitted = isCommitted;
            this.isDeleted = isDeleted;
            this.binaryKey = binaryKey;
        }
        /**
         * @return true if the entry is committed
         */
        public boolean isCommitted() {
            return isCommitted;
        }
        /**
         * @return true if the entry represents a delete
         */
        public boolean isDeleted() {
            return isDeleted;
        }
        /**
         * mark the entry as committed.
         */
        public void commit() {
            this.isCommitted = true;
        }
        /**
         * @return the version cache key this entry points to
         */
        public Binary getBinaryKey() {
            return binaryKey;
        }
        
    }

    private ConcurrentMap<K, NavigableMap<TransactionId, IndexEntry>> index
        = new ConcurrentHashMap<K, NavigableMap<TransactionId, IndexEntry>>();
    
    private BackingMapContext bmc;
    
    /**
     * Constructor.
     * @param bmc the backing map context for the cache
     */
    public MVCCIndex(final BackingMapContext bmc) {
        this.bmc = bmc;
    }

    /**
     * Get the set of version cache keys representing the current versions of the
     * entries represented in the candidate set. Any uncommitted entries older
     * than the current version and newer than the most recent committed entry are
     * included
     * @param candidateSet set of candidate version cache keys
     * @param ts transaction id
     * @return the set of matching version keys
     */
    public Set<Binary> floorSet(final Set<Binary> candidateSet, final TransactionId ts) {
        Set<Binary> result = new HashSet<Binary>();
        for (Binary candidate : candidateSet) {
            Converter converter = bmc.getManagerContext().getKeyFromInternalConverter();
            @SuppressWarnings("unchecked")
            VersionedKey<K> vk = (VersionedKey<K>) converter.convert(candidate);
            Entry<TransactionId, IndexEntry> floorEntry = floorEntry(vk.getNativeKey(), ts);
            if (floorEntry != null) {
                if (!floorEntry.getValue().isDeleted()) {
                    result.add(floorEntry.getValue().getBinaryKey());
                }
                while (floorEntry != null && !floorEntry.getValue().isCommitted()) {
                    floorEntry = nextFloor(vk.getNativeKey(), floorEntry.getKey());
                    if (floorEntry != null && !floorEntry.getValue().isDeleted()) {
                        result.add(floorEntry.getValue().getBinaryKey());
                    }
                }
            }
        }
        return result;
    }
    
    /**
     * Get the current version of a key at the given timestamp. Keys
     * with the same or lower transaction id.
     * @param sKey the key
     * @param ts the transaction id
     * @return the matching version key, or null
     */
    public Binary floor(final K sKey, final TransactionId ts) {
         Entry<TransactionId, IndexEntry> floorEntry = floorEntry(sKey, ts);
        return floorEntry == null ? null : floorEntry.getValue().getBinaryKey();
    }
    
    /**
     * Get the version key preceding the given timestamp.
     * @param sKey the key
     * @param ts the transaction id
     * @return the next lower version key, or null
     */
    public Binary lower(final K sKey, final TransactionId ts) {
         Entry<TransactionId, IndexEntry> lowerEntry = lowerEntry(sKey, ts);
        return lowerEntry == null ? null : lowerEntry.getValue().getBinaryKey();
    }

    /**
     * Get the index map entry with the current version of a key at the given timestamp. Keys
     * with the same or lower transaction id.
     * @param sKey the key
     * @param ts the transaction id
     * @return the index map entry
     */
    public Entry<TransactionId, IndexEntry> floorEntry(final K sKey, final TransactionId ts) {
        NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
        if (line != null) {
            synchronized (line) {
                if (ts == null) {
                    return line.firstEntry();
                } else {
                    return line.floorEntry(ts);
                }
            }
        } else {
            return null;
        }
    }
    
    /**
     * Get the index map entry for the timestamp preceding the one given.
     * @param sKey logical key
     * @param ts transaction id
     * @return the next older map entry
     */
    public Entry<TransactionId, IndexEntry> lowerEntry(final K sKey, final TransactionId ts) {
        NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
        if (line != null) {
            synchronized (line) {
                if (ts == null) {
                    return line.lastEntry();
                } else {
                    return line.lowerEntry(ts);
                }
            }
        } else {
            return null;
        }
    }

    /**
     * Get the index map entry for the next timestamp later than the one given.
     * @param sKey logical key
     * @param ts transaction id
     * @return the next later map entry
     */
    public Entry<TransactionId, IndexEntry> higherEntry(final K sKey, final TransactionId ts) {
        NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
        if (line != null) {
            synchronized (line) {
                if (ts == null) {
                    return line.firstEntry();
                } else {
                    return line.higherEntry(ts);
                }
            }
        } else {
            return null;
        }
    }
    
    /**
     * @param sKey the key 
     * @param ts the transaction id
     * @return next lower entry
     */
    //TODO this is redundant - replace with lowerEntry()
    private Entry<TransactionId, IndexEntry> nextFloor(final K sKey, final TransactionId ts) {
        NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
        if (line != null && ts != null) {
            synchronized (line) {
                TransactionId lowerts = line.lowerKey(ts);
                if (lowerts == null) {
                    return null;
                } else {
                    return new AbstractMap.SimpleEntry<TransactionId, IndexEntry>(lowerts, line.get(lowerts));
                }
            }
        } else {
            return null;
        }
    }
    
    /**
     * Get the next later transaction id.
     * @param sKey the key
     * @param ts current transaction id
     * @return the next transaction id
     */
    public TransactionId ceilingTid(final K sKey, final TransactionId ts) {
        NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
        if (line != null) {
            synchronized (line) {
                if (ts == null) {
                    Entry<TransactionId, IndexEntry> tsl = line.lastEntry();
                    return tsl == null ? null : tsl.getKey();
                } else {
                    Entry<TransactionId, IndexEntry> tsl = line.ceilingEntry(ts);
                    return tsl == null ? null : tsl.getKey();
                }
            }
        } else {
            return null;
        }
    }
        
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void insert(final Entry entry) {
        K sKey;        
        TransactionId ts;
        if (!(entry instanceof BinaryEntry)) {
            throw new UnsupportedOperationException("only binary entry supported");
        }
        sKey = (K) Constants.KEYEXTRACTOR.extractFromEntry(entry);
        ts = (TransactionId) Constants.TXEXTRACTOR.extractFromEntry(entry);
        boolean committed = Utils.isCommitted((BinaryEntry) entry);
        boolean deleted = Utils.isDeleted((BinaryEntry) entry);
        Binary binaryKey = ((BinaryEntry) entry).getBinaryKey();
        addToIndex(sKey, ts, binaryKey, committed, deleted);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void delete(final Entry entry) {
        K sKey;        
        TransactionId ts;
        if (entry instanceof BinaryEntry) {
            sKey = (K) Constants.KEYEXTRACTOR.extractFromEntry(entry);
            ts = (TransactionId) Constants.TXEXTRACTOR.extractFromEntry(entry);
        } else {
            VersionedKey<K> key = (VersionedKey<K>) entry.getKey();        
            sKey = key.getNativeKey();        
            ts = key.getTxTimeStamp();
        }
        removeFromIndex(sKey, ts);
    }
    
    /**
     * Add an entry to the index.
     * @param sKey the logical key 
     * @param ts transaction id to add
     * @param binaryKey version cache binary key
     * @param committed committed flag
     * @param deleted deleted flag
     */
    private void addToIndex(final K sKey, final TransactionId ts,
            final Binary binaryKey, final boolean committed, final boolean deleted) {
        while (true) {
            NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
            synchronized (line) {
                line.put(ts, new IndexEntry(committed, deleted, binaryKey));
                if (line == getLine(sKey)) {
                    return;
                }
            }                    
        }
    }
    
    /**
     * Update the index reflecting a changed value. The only permissible
     * change is for an uncommitted entry to become committed. All other changes 
     * are represented by new and deleted entries
     * @param sKey The logical key
     * @param ts transaction id
     * @param committed the committed value (redundant - always true)
     */
    private void updateIndex(final K sKey, final TransactionId ts, final boolean committed) {
        NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
        synchronized (line) {
            if (committed) {
                line.get(ts).commit();
            }
        }                    
    }
    
    /**
     * Delete an entry from the index. Can only be from rollback or purge of old version.
     * @param sKey logical key
     * @param ts transaction id
     */
    private void removeFromIndex(final K sKey, final TransactionId ts) {
        while (true) {
            NavigableMap<TransactionId, IndexEntry> line = getLine(sKey);
            synchronized (line) {
                line.remove(ts);
                if (line.isEmpty()) {
                    if (index.remove(sKey, line)) {
                        return;
                    }
                } else if (line == getLine(sKey)) {
                    return;
                }
            }                    
        }
    }

    /**
     * Get the map for a specified logical key.
     * @param sKey the key
     * @return the map
     */
    private NavigableMap<TransactionId, IndexEntry> getLine(final K sKey) {
        while (true) {
            NavigableMap<TransactionId, IndexEntry> line = index.get(sKey);
            if (line == null) {
                line = new TreeMap<TransactionId, IndexEntry>();
                index.putIfAbsent(sKey, line);
            } else {
                return line;
            }
        }
    }
    
    @Override
    public Object get(final Object oKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Comparator getComparator() {
        return null;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map getIndexContents() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueExtractor getValueExtractor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOrdered() {
        return false;
    }

    @Override
    public boolean isPartial() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void update(@SuppressWarnings("rawtypes") final Entry entry) {
        if (!(entry instanceof BinaryEntry)) {
            throw new UnsupportedOperationException("only binary entry supported");
        }
        K sKey = (K) Constants.KEYEXTRACTOR.extractFromEntry(entry);
        TransactionId ts = (TransactionId) Constants.TXEXTRACTOR.extractFromEntry(entry);
        Boolean committed = Utils.isCommitted((BinaryEntry) entry);
        updateIndex(sKey, ts, committed);
    }
}
