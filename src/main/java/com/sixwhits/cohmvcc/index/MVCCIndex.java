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
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Converter;
import com.tangosol.util.MapIndex;
import com.tangosol.util.ValueExtractor;

/**
 * @author David Whitmarsh, based on an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class MVCCIndex<K> implements MapIndex {
	
	private static class IndexEntry {
		private boolean isCommitted;
		private Binary binaryKey;
		public IndexEntry(boolean isCommitted, Binary binaryKey) {
			super();
			this.isCommitted = isCommitted;
			this.binaryKey = binaryKey;
		}
		public boolean isCommitted() {
			return isCommitted;
		}
		public void commit() {
			this.isCommitted = true;
		}
		public Binary getBinaryKey() {
			return binaryKey;
		}
		
	}

	private ConcurrentMap<K, NavigableMap<TransactionId,IndexEntry>> index = new ConcurrentHashMap<K, NavigableMap<TransactionId,IndexEntry>>();
	
	private BackingMapContext bmc;
	
	public MVCCIndex(BackingMapContext bmc) {
		this.bmc = bmc;
	}

	public Set<Binary> floorSet(Set<Binary> candidateSet, TransactionId ts) {
		Set<Binary> result = new HashSet<Binary>();
		for (Binary candidate : candidateSet) {
			Converter converter = bmc.getManagerContext().getKeyFromInternalConverter();
			@SuppressWarnings("unchecked")
			VersionedKey<K> vk = (VersionedKey<K>) converter.convert(candidate);
			Entry<TransactionId,IndexEntry> floorEntry = floorEntry(vk.getNativeKey(), ts);
			if (floorEntry != null) {
				result.add(floorEntry.getValue().getBinaryKey());
				while (floorEntry != null && !floorEntry.getValue().isCommitted()) {
					floorEntry = nextFloor(vk.getNativeKey(), floorEntry.getKey());
					if (floorEntry != null) {
						result.add(floorEntry.getValue().getBinaryKey());
					}
				}
			}
		}
		return result;
	}
	
	public Binary floor(K sKey, TransactionId ts) {
		 Entry<TransactionId,IndexEntry> floorEntry = floorEntry(sKey, ts);
		return floorEntry == null ? null : floorEntry.getValue().getBinaryKey();
	}

	public Entry<TransactionId,IndexEntry> floorEntry(K sKey, TransactionId ts) {
	    NavigableMap<TransactionId,IndexEntry> line = getLine(sKey);
		if (line != null) {
			synchronized(line) {
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
	
	private Entry<TransactionId,IndexEntry> nextFloor(K sKey, TransactionId ts) {
	    NavigableMap<TransactionId,IndexEntry> line = getLine(sKey);
		if (line != null && ts != null) {
			synchronized(line) {
				TransactionId lowerts = line.lowerKey(ts);
				if (lowerts == null) {
					return null;
				} else {
					return new AbstractMap.SimpleEntry<TransactionId,IndexEntry>(lowerts, line.get(lowerts));
				}
			}
		} else {
			return null;
		}
	}
	
//	private Binary getBinaryKey(K sKey, TransactionId ts) {
//	    NavigableMap<TransactionId,Binary> line = getLine(sKey);
//	    return line == null ? null : line.get(ts);
//		
//	}
	
	public TransactionId ceilingTid(K sKey, TransactionId ts) {
	    NavigableMap<TransactionId,IndexEntry> line = getLine(sKey);
		if (line != null) {
			synchronized(line) {
				if (ts == null) {
                    Entry<TransactionId,IndexEntry> tsl = line.lastEntry();
                    return tsl == null ? null : tsl.getKey();
				} else {
					Entry<TransactionId,IndexEntry> tsl = line.ceilingEntry(ts);
                    return tsl == null ? null : tsl.getKey();
				}
			}
		} else {
			return null;
		}
	}
		
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void insert(Entry entry) {
		K sKey;		
		TransactionId ts;
		if (!(entry instanceof BinaryEntry)) {
			throw new UnsupportedOperationException("only binary entry supported");
		}
		sKey = (K) Constants.KEYEXTRACTOR.extractFromEntry(entry);
		ts = (TransactionId) Constants.TXEXTRACTOR.extractFromEntry(entry);
		Boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(entry);
		Binary binaryKey = ((BinaryEntry)entry).getBinaryKey();
		addToIndex(sKey, ts, binaryKey, committed);
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void delete(Entry entry) {
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
	
	private void addToIndex(K sKey, TransactionId ts, Binary binaryKey, Boolean committed) {
		while(true) {
			NavigableMap<TransactionId,IndexEntry> line = getLine(sKey);
			synchronized(line) {
				line.put(ts, new IndexEntry(committed, binaryKey));
				if (line == getLine(sKey)) {
					return;
				}
			}					
		}
	}
	private void updateIndex(K sKey, TransactionId ts, Boolean committed) {
		NavigableMap<TransactionId,IndexEntry> line = getLine(sKey);
		synchronized(line) {
			if (committed) {
				line.get(ts).commit();
			}
		}					
	}
	
	private void removeFromIndex(K sKey, TransactionId ts) {
		while(true) {
		    NavigableMap<TransactionId,IndexEntry> line = getLine(sKey);
			synchronized(line) {
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

	private NavigableMap<TransactionId,IndexEntry> getLine(K sKey) {
		while(true) {
		    NavigableMap<TransactionId,IndexEntry> line = index.get(sKey);
			if (line == null) {
				line = new TreeMap<TransactionId,IndexEntry>();
				index.putIfAbsent(sKey, line);
			} else {
				return line;
			}
		}
	}
	
	@Override
    public Object get(Object oKey) {
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
    public void update(@SuppressWarnings("rawtypes") Entry entry) {
		if (!(entry instanceof BinaryEntry)) {
			throw new UnsupportedOperationException("only binary entry supported");
		}
		K sKey = (K) Constants.KEYEXTRACTOR.extractFromEntry(entry);
		TransactionId ts = (TransactionId) Constants.TXEXTRACTOR.extractFromEntry(entry);
		Boolean committed = (Boolean) Constants.COMMITSTATUSEXTRACTOR.extractFromEntry(entry);
		updateIndex(sKey, ts, committed);
    }
}
