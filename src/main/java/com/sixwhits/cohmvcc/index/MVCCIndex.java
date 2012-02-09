package com.sixwhits.cohmvcc.index;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Converter;
import com.tangosol.util.MapIndex;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;

/**
 * @author David Whitmarsh, based on an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class MVCCIndex<K> implements MapIndex {

	private ConcurrentMap<K, NavigableMap<TransactionId,Binary>> index = new ConcurrentHashMap<K, NavigableMap<TransactionId,Binary>>();
	
	private BackingMapContext bmc;
	private PofExtractor keyExtractor = new PofExtractor(null, new SimplePofPath(VersionedKey.POF_KEY), AbstractExtractor.KEY);
	private PofExtractor tsExtractor = new PofExtractor(null, new SimplePofPath(VersionedKey.POF_TX), AbstractExtractor.KEY);
	
	public MVCCIndex(BackingMapContext bmc) {
		this.bmc = bmc;
	}

	public Set<Binary> floorSet(Set<Binary> candidateSet, TransactionId ts) {
		Set<Binary> result = new HashSet<Binary>();
		for (Binary candidate : candidateSet) {
			Converter converter = bmc.getManagerContext().getKeyFromInternalConverter();
			@SuppressWarnings("unchecked")
			VersionedKey<K> vk = (VersionedKey<K>) converter.convert(candidate);
			Binary floorKey = floor(vk.getNativeKey(), ts);
			if (floorKey != null) {
				result.add(floorKey);
			}
		}
		return result;
	}
	
	public Binary floor(K sKey, TransactionId ts) {
	    NavigableMap<TransactionId,Binary> line = getLine(sKey);
		if (line != null) {
			synchronized(line) {
				if (ts == null) {
                    Entry<TransactionId,Binary> tsl = line.firstEntry();
                    return tsl == null ? null : tsl.getValue();
				} else {
					Entry<TransactionId,Binary> tsl = line.floorEntry(ts);
                    return tsl == null ? null : tsl.getValue();
				}
			}
		} else {
			return null;
		}
	}
	public TransactionId ceilingTid(K sKey, TransactionId ts) {
	    NavigableMap<TransactionId,Binary> line = getLine(sKey);
		if (line != null) {
			synchronized(line) {
				if (ts == null) {
                    Entry<TransactionId,Binary> tsl = line.lastEntry();
                    return tsl == null ? null : tsl.getKey();
				} else {
					Entry<TransactionId,Binary> tsl = line.ceilingEntry(ts);
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
		sKey = (K) keyExtractor.extractFromEntry(entry);
		ts = (TransactionId) tsExtractor.extractFromEntry(entry);
		Binary binaryKey = ((BinaryEntry)entry).getBinaryKey();
		addToIndex(sKey, ts, binaryKey);
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void delete(Entry entry) {
		K sKey;		
		TransactionId ts;
		if (entry instanceof BinaryEntry) {
			sKey = (K) keyExtractor.extractFromEntry(entry);
			ts = (TransactionId) tsExtractor.extractFromEntry(entry);
		} else {
	        VersionedKey<K> key = (VersionedKey<K>) entry.getKey();		
			sKey = key.getNativeKey();		
			ts = key.getTxTimeStamp();
		}
		removeFromIndex(sKey, ts);
	}
	
	private void addToIndex(K sKey, TransactionId ts, Binary binaryKey) {
		while(true) {
			NavigableMap<TransactionId,Binary> line = getLine(sKey);
			synchronized(line) {
				line.put(ts, binaryKey);
				if (line == getLine(sKey)) {
					return;
				}
			}					
		}
	}
	
	private void removeFromIndex(K sKey, TransactionId ts) {
		while(true) {
		    NavigableMap<TransactionId,Binary> line = getLine(sKey);
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

	private NavigableMap<TransactionId,Binary> getLine(K sKey) {
		while(true) {
		    NavigableMap<TransactionId,Binary> line = index.get(sKey);
			if (line == null) {
				line = new TreeMap<TransactionId,Binary>();
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

	@Override
    public void update(@SuppressWarnings("rawtypes") Entry entry) {
		// it is not permitted for the index value (timestamp) for an existing entry to change
        return;
    }
}
