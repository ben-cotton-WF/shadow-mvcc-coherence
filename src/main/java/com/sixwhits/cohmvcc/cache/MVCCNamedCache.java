package com.sixwhits.cohmvcc.cache;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.transaction.Context;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.CacheService;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;

public class MVCCNamedCache implements NamedCache {

	private final Context transactionContext;
	private final String cacheName;
	private final NamedCache keyCache;
	private final NamedCache versionCache;
	
	
	public MVCCNamedCache(Context transactionContext, String cacheName) {
		super();
		this.transactionContext = transactionContext;
		this.cacheName = cacheName;
		this.keyCache = CacheFactory.getCache(cacheName + "-keys");
		this.versionCache = CacheFactory.getCache(cacheName + "-versions");
	}
	
	public Object get(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	public void addMapListener(MapListener listener) {
		// TODO Auto-generated method stub

	}

	public void addMapListener(MapListener listener, Object oKey, boolean fLite) {
		// TODO Auto-generated method stub

	}

	public void addMapListener(MapListener listener, Filter filter,
			boolean fLite) {
		// TODO Auto-generated method stub

	}

	public void removeMapListener(MapListener listener) {
		// TODO Auto-generated method stub

	}

	public void removeMapListener(MapListener listener, Object oKey) {
		// TODO Auto-generated method stub

	}

	public void removeMapListener(MapListener listener, Filter filter) {
		// TODO Auto-generated method stub

	}

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean containsKey(Object key) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	public Object put(Object key, Object value) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object remove(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	public void putAll(Map m) {
		// TODO Auto-generated method stub

	}

	public void clear() {
		// TODO Auto-generated method stub

	}

	public Set keySet() {
		// TODO Auto-generated method stub
		return null;
	}

	public Collection values() {
		// TODO Auto-generated method stub
		return null;
	}

	public Set entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	public Map getAll(Collection colKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean lock(Object oKey) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean lock(Object oKey, long cWait) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean unlock(Object oKey) {
		// TODO Auto-generated method stub
		return false;
	}

	public void addIndex(ValueExtractor extractor, boolean fOrdered,
			Comparator comparator) {
		// TODO Auto-generated method stub

	}

	public Set entrySet(Filter filter) {
		// TODO Auto-generated method stub
		return null;
	}

	public Set entrySet(Filter filter, Comparator comparator) {
		// TODO Auto-generated method stub
		return null;
	}

	public Set keySet(Filter filter) {
		// TODO Auto-generated method stub
		return null;
	}

	public void removeIndex(ValueExtractor extractor) {
		// TODO Auto-generated method stub

	}

	public Object aggregate(Collection collKeys, EntryAggregator agent) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object aggregate(Filter filter, EntryAggregator agent) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object invoke(Object oKey, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	public Map invokeAll(Collection collKeys, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	public Map invokeAll(Filter filter, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	public void destroy() {
		// TODO Auto-generated method stub

	}

	public String getCacheName() {
		// TODO Auto-generated method stub
		return null;
	}

	public CacheService getCacheService() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isActive() {
		// TODO Auto-generated method stub
		return false;
	}

	public Object put(Object oKey, Object oValue, long cMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	public void release() {
		// TODO Auto-generated method stub

	}

}
