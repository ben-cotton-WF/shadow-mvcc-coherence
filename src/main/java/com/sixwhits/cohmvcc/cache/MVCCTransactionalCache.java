package com.sixwhits.cohmvcc.cache;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.net.CacheService;
import com.tangosol.util.Filter;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;

public interface MVCCTransactionalCache<K,V> {

	public abstract V get(TransactionId tid,
			IsolationLevel isolationLevel, K key);

	public abstract V put(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key, V value);

	/**
	 * Like put, but doesn't return old value so no read registered.
	 * @param tid
	 * @param isolationLevel
	 * @param autoCommit
	 * @param key
	 * @param value
	 * @return
	 */
	public void insert(TransactionId tid, boolean autoCommit, K key, V value);

	public abstract <R> R invoke(TransactionId tid,
			IsolationLevel isolationLevel, boolean autoCommit, K oKey, EntryProcessor agent);

	public abstract void addMapListener(MapListener listener, TransactionId tid, IsolationLevel isolationLevel);

	public abstract void addMapListener(MapListener listener,
			TransactionId tid, IsolationLevel isolationLevel, Object oKey, boolean fLite);

	public abstract void addMapListener(MapListener listener,
			TransactionId tid, IsolationLevel isolationLevel, Filter filter, boolean fLite);

	public abstract void removeMapListener(MapListener listener);

	public abstract void removeMapListener(MapListener listener, Object oKey);

	public abstract void removeMapListener(MapListener listener, Filter filter);

	public abstract int size(TransactionId tid, IsolationLevel isolationLevel);

	public abstract boolean isEmpty(TransactionId tid,
			IsolationLevel isolationLevel);

	public abstract boolean containsKey(TransactionId tid,
			IsolationLevel isolationLevel, K key);

	public abstract boolean containsValue(TransactionId tid,
			IsolationLevel isolationLevel, V value);

	public abstract V remove(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key);

	public abstract void putAll(TransactionId tid, boolean autoCommit, Map<K, V> m);

	public abstract void clear(TransactionId tid, boolean autoCommit);

	public abstract Set<K> keySet(TransactionId tid, IsolationLevel isolationLevel);

	public abstract Collection<V> values(TransactionId tid,
			IsolationLevel isolationLevel);

	public abstract Set<Map.Entry<K,V>> entrySet(TransactionId tid,
			IsolationLevel isolationLevel);

	public abstract Map<K, V> getAll(TransactionId tid,
			IsolationLevel isolationLevel, Collection<K> colKeys);

	public abstract void addIndex(ValueExtractor extractor, boolean fOrdered,
			Comparator<V> comparator);

	public abstract Set<Map.Entry<K, V>> entrySet(TransactionId tid,
			IsolationLevel isolationLevel, Filter filter);

	public abstract Set<Map.Entry<K, V>> entrySet(TransactionId tid,
			IsolationLevel isolationLevel, Filter filter, Comparator<V> comparator);

	public abstract Set<K> keySet(TransactionId tid,
			IsolationLevel isolationLevel, Filter filter);

	public abstract void removeIndex(ValueExtractor extractor);

	public abstract <R> R aggregate(TransactionId tid,
			IsolationLevel isolationLevel, Collection<K> collKeys,
			EntryAggregator agent);

	public abstract <R> R aggregate(TransactionId tid,
			IsolationLevel isolationLevel, Filter filter, EntryAggregator agent);

	public abstract <R> Map<K, R> invokeAll(TransactionId tid,
			IsolationLevel isolationLevel, boolean autoCommit, Collection<K> collKeys,
			EntryProcessor agent);

	public abstract <R> Map<K, R> invokeAll(TransactionId tid,
			IsolationLevel isolationLevel, boolean autoCommit, Filter filter, EntryProcessor agent);

	public abstract void destroy();

	public abstract String getCacheName();

	public abstract CacheService getCacheService();

	public abstract boolean isActive();

	public abstract void release();

}