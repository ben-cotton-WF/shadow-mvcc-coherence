package com.sixwhits.cohmvcc.cache.internal;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.MVCCTransactionalCache;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.CacheService;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;

public class MVCCTransactionalCacheImpl implements MVCCTransactionalCache {

	private final String cacheName;
	private final NamedCache keyCache;
	private final NamedCache versionCache;
	
	
	public MVCCTransactionalCacheImpl(String cacheName) {
		super();
		this.cacheName = cacheName;
		this.keyCache = CacheFactory.getCache(cacheName + "-keys");
		this.versionCache = CacheFactory.getCache(cacheName + "-versions");
	}
	
	@Override
	public Object get(TransactionId tid, IsolationLevel isolationLevel, Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addMapListener(MapListener listener, TransactionId tid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addMapListener(MapListener listener, TransactionId tid, Object oKey, boolean fLite) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addMapListener(MapListener listener, TransactionId tid, Filter filter,
			boolean fLite) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeMapListener(MapListener listener, TransactionId tid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeMapListener(MapListener listener, TransactionId tid, Object oKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeMapListener(MapListener listener, TransactionId tid, Filter filter) {
		// TODO Auto-generated method stub

	}

	@Override
	public int size(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isEmpty(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsKey(TransactionId tid, IsolationLevel isolationLevel, Object key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsValue(TransactionId tid, IsolationLevel isolationLevel, Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object put(TransactionId tid, Object key, Object value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object remove(TransactionId tid, Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putAll(TransactionId tid, Map m) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clear(TransactionId tid) {
		// TODO Auto-generated method stub

	}

	@Override
	public Set keySet(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection values(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set entrySet(TransactionId tid, IsolationLevel isolationLevel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map getAll(TransactionId tid, IsolationLevel isolationLevel, Collection colKeys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addIndex(ValueExtractor extractor, boolean fOrdered,
			Comparator comparator) {
		// TODO Auto-generated method stub

	}

	@Override
	public Set entrySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set entrySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter, Comparator comparator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set keySet(TransactionId tid, IsolationLevel isolationLevel, Filter filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeIndex(ValueExtractor extractor) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object aggregate(TransactionId tid, IsolationLevel isolationLevel, Collection collKeys, EntryAggregator agent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object aggregate(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryAggregator agent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object invoke(TransactionId tid, IsolationLevel isolationLevel, Object oKey, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map invokeAll(TransactionId tid, IsolationLevel isolationLevel, Collection collKeys, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map invokeAll(TransactionId tid, IsolationLevel isolationLevel, Filter filter, EntryProcessor agent) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCacheName() {
		return cacheName;
	}

	@Override
	public CacheService getCacheService() {
		return versionCache.getCacheService();
	}

	@Override
	public boolean isActive() {
		return versionCache.isActive();
	}

	@Override
	public Object put(TransactionId tid, Object oKey, Object oValue, long cMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void release() {
		keyCache.release();
		versionCache.release();
	}

}
