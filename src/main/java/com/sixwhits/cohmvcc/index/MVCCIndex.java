/**
 * Copyright 2011 Alexey Ragozin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sixwhits.cohmvcc.index;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.MapIndex;
import com.tangosol.util.ValueExtractor;

/**
 * @author David Whitmarsh, based on an idea by Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class MVCCIndex<K> implements MapIndex {

	private ConcurrentMap<K, NavigableSet<TransactionId>> index = new ConcurrentHashMap<K, NavigableSet<TransactionId>>();
	
	@SuppressWarnings("unused")
	private BackingMapContext bmc;
	
	public MVCCIndex(BackingMapContext bmc) {
		this.bmc = bmc; 
	}

	public Set<K> getSeriesKeys() {
		return index.keySet();
	}
	
	public NavigableSet<TransactionId> getSeries(K sKey) {
	    NavigableSet<TransactionId> line = getLine(sKey);
		if (line != null) {
			synchronized (line) {
				return new TreeSet<TransactionId>(line);
			}
		}
		else {
			return null;
		}
	}

	public VersionedKey<K> ceiling(K sKey, TransactionId ts) {
		NavigableSet<TransactionId> line = getLine(sKey);
		if (line != null) {
			synchronized(line) {
				if (ts == null) {
				    TransactionId tsl = line.last();
				    return tsl == null ? null : new VersionedKey<K>(sKey, tsl);
				} else {
					TransactionId tsl = line.ceiling(ts);
                    return tsl == null ? null : new VersionedKey<K>(sKey, tsl);
				}
			}
		}
		else {
			return null;
		}
	}

	public VersionedKey<K> floor(K sKey, TransactionId ts) {
	    NavigableSet<TransactionId> line = getLine(sKey);
		if (line != null) {
			synchronized(line) {
				if (ts == null) {
                    TransactionId tsl = line.first();
                    return tsl == null ? null : new VersionedKey<K>(sKey, tsl);
				} else {
                    TransactionId tsl = line.floor(ts);
                    return tsl == null ? null : new VersionedKey<K>(sKey, tsl);
				}
			}
		}
		else {
			return null;
		}
	}
		
	@SuppressWarnings("rawtypes")
	public void insert(Entry entry) {
		@SuppressWarnings("unchecked")
        VersionedKey<K> key = (VersionedKey<K>) entryKey(entry);		
		K sKey = key.getNativeKey();		
		TransactionId ts = key.getTxTimeStamp();
		addToIndex(sKey, ts);
	}

	@SuppressWarnings("rawtypes")
	public void delete(Entry entry) {
        @SuppressWarnings("unchecked")
        VersionedKey<K> key = (VersionedKey<K>) entryKey(entry);        
        K sKey = key.getNativeKey();        
        TransactionId ts = key.getTxTimeStamp();
		removeFromIndex(sKey, ts);
	}
	
	private void addToIndex(K sKey, TransactionId ts) {
		while(true) {
			NavigableSet<TransactionId> line = getLine(sKey);
			synchronized(line) {
				line.add(ts);
				if (line == getLine(sKey)) {
					return;
				}
			}					
		}
	}
	
	private void removeFromIndex(K sKey, TransactionId ts) {
		while(true) {
		    NavigableSet<TransactionId> line = getLine(sKey);
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

	private NavigableSet<TransactionId> getLine(K sKey) {
		while(true) {
		    NavigableSet<TransactionId> line = index.get(sKey);
			if (line == null) {
				line = new TreeSet<TransactionId>();
				index.putIfAbsent(sKey, line);
			} else {
				return line;
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private Object entryKey(Entry entry) {
		if (entry instanceof BinaryEntry) {
			return ((BinaryEntry)entry).getBinaryKey();
		}
		else {
			return entry.getKey();
		}
	}
	
    public Object get(Object oKey) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("rawtypes")
    public Comparator getComparator() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    public Map getIndexContents() {
        throw new UnsupportedOperationException();
    }

    public ValueExtractor getValueExtractor() {
        throw new UnsupportedOperationException();
    }

    public boolean isOrdered() {
        return false;
    }

    public boolean isPartial() {
        return false;
    }

    public void update(@SuppressWarnings("rawtypes") Entry entry) {
        throw new UnsupportedOperationException();
    }
}
