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

package com.shadowmvcc.coherence.cache;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import com.shadowmvcc.coherence.cache.internal.InvocationFinalResult;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.net.CacheService;
import com.tangosol.util.Filter;
import com.tangosol.util.MapListener;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.InvocableMap.EntryAggregator;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Interface defining operations on the logical cache with transaction
 * context information given with each request. Isolation level determines whether a read marker
 * is recorded when a value is read, and whether uncommitted values can be read. Operations with isolation
 * level {@code readCommitted} or higher will block on encountering an uncommitted version.
 * 
 * This is the potential decoupling point for extend clients. Cluster members would call
 * an implementation directly, extend clients a facade implementation to use invocation service.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public interface MVCCTransactionalCache<K, V> {

    /**
     * key based get.
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level isolation level 
     * @param key the cache key the key
     * @return cache value
     */
    V get(TransactionId tid, 
            IsolationLevel isolationLevel, K key);

    /**
     * put. Subject to isolation level this will place a read marker. Use insert or putAll
     * to update without a read marker
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level isolation level 
     * @param autoCommit implicit commit if true implicit commit if true
     * @param key the cache key key
     * @param value the value new value
     * @return old value.
     */
    V put(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key, V value);

    /**
     * Like put, but doesn't return old value so no read registered.
     * @param tid transaction id transaction id
     * @param autoCommit implicit commit if true true to commit immediately
     * @param key the cache key key to insert
     * @param value the value value to insert
     */
    void insert(TransactionId tid, boolean autoCommit, K key, V value);

    /**
     * Invoke an EntryProcessor based on the logical cache design.
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level isolation level 
     * @param autoCommit implicit commit if true implicit commit if true
     * @param readonly invoke as read only
     * @param oKey key
     * @param agent the processor
     * @return a map of key, result pairs, with the set of keys changed
     * @param <R> return type of the EntryProcessor
     */
    <R> InvocationFinalResult<K, R> invoke(TransactionId tid, 
            IsolationLevel isolationLevel, boolean autoCommit, boolean readonly, K oKey, EntryProcessor agent);

    /**
     * Add a MapListener that will receive logical cache events. Events with timestamps
     * older than the transaction id will be ignored. uncommitted events will be received if
     * isolationLevel is {@code uncommitted}
     * @param listener the map listener
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level isolation level 
     */
    void addMapListener(MapListener listener, TransactionId tid, IsolationLevel isolationLevel);

    /**
     * Add a MapListener for the given key that will receive logical cache events. Events with timestamps
     * older than the transaction id will be ignored. uncommitted events will be received if
     * isolationLevel is {@code uncommitted}
     * @param listener the map listener
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level isolation level 
     * @param oKey the key
     * @param fLite allow light events
     */
    void addMapListener(MapListener listener, 
            TransactionId tid, IsolationLevel isolationLevel, Object oKey, boolean fLite);

    /**
     * Add a MapListener for the given filter that will receive logical cache events. Events with timestamps
     * older than the transaction id will be ignored. uncommitted events will be received if
     * isolationLevel is {@code uncommitted}.
     * @param listener the map listener
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level isolation level 
     * @param filter the filter to receive events for
     * @param fLite allow light events
     */
    void addMapListener(MapListener listener, 
            TransactionId tid, IsolationLevel isolationLevel, Filter filter, boolean fLite);

    /**
     * Remove the previously registered whole-cache listener.
     * @param listener the listener
     */
    void removeMapListener(MapListener listener);

    /**
     * Remove the previously registered listener on a key.
     * @param listener the listener
     * @param oKey the key
     */
    void removeMapListener(MapListener listener, Object oKey);

    /**
     * Remove the previously registered listener against a filter.
     * @param listener the listener
     * @param filter the filter
     */
    void removeMapListener(MapListener listener, Filter filter);

    /**
     * Find the size of the cache as at the specified transaction id.
     * @param tid transaction id the transaction id
     * @param isolationLevel isolation level the isolation level
     * @return the cache size
     */
    int size(TransactionId tid, IsolationLevel isolationLevel);

    /**
     * Determine if the cache is empty at the timestamp.
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level isolation level
     * @return true if the cache has no current entries
     */
    boolean isEmpty(TransactionId tid, 
            IsolationLevel isolationLevel);

    /**
     * Determine if the cache contains a specific key.
     * @param tid transaction id transaction id 
     * @param isolationLevel isolation level
     * @param key the cache key
     * @return true if the key is present at the timestamp
     */
    boolean containsKey(TransactionId tid, 
            IsolationLevel isolationLevel, K key);

    /**
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level
     * @param value the value
     * @return true if the value is present at the timestamp
     */
    boolean containsValue(TransactionId tid, 
            IsolationLevel isolationLevel, V value);

    /**
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level
     * @param autoCommit implicit commit if true
     * @param key the cache key
     * @return the previous (next most recent) value, or null
     */
    V remove(TransactionId tid, IsolationLevel isolationLevel, boolean autoCommit, K key);

    /**
     * @param tid transaction id transaction id
     * @param autoCommit implicit commit if true
     * @param m map of key value pairs to store
     */
    void putAll(TransactionId tid, boolean autoCommit, Map<K, V> m);

    /**
     * Clear the cache at the given transaction id by creating a new deleted marker for every extant entry.
     * @param tid transaction id transaction id
     * @param autoCommit implicit commit if true
     */
    void clear(TransactionId tid, boolean autoCommit);

    /**
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level
     * @return all the extant keys at the timestamp
     */
    Set<K> keySet(TransactionId tid, IsolationLevel isolationLevel);

    /**
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level
     * @return all the extant values at the timestamp
     */
    Collection<V> values(TransactionId tid, 
            IsolationLevel isolationLevel);

    /**
     * @param tid transaction id transaction id
     * @param isolationLevel isolation level
     * @return all the extant entries at the timestamp
     */
    Set<Map.Entry<K, V>> entrySet(TransactionId tid, 
            IsolationLevel isolationLevel);

    /**
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param colKeys the keys to get values for
     * @return the map of extant key value pairs
     */
    Map<K, V> getAll(TransactionId tid, 
            IsolationLevel isolationLevel, Collection<K> colKeys);

    /**
     * Add an index to the version cache.
     * @param extractor value extractor
     * @param fOrdered is the index ordered
     * @param comparator comparator used for ordering, or null
     */
    void addIndex(ValueExtractor extractor, boolean fOrdered, 
            Comparator<V> comparator);

    /**
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param filter the filter
     * @return the set of extant entries at timestamp matching the filter
     */
    Set<Map.Entry<K, V>> entrySet(TransactionId tid, 
            IsolationLevel isolationLevel, Filter filter);

    /**
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param filter the filter
     * @param comparator comparator for ordering results
     * @return extant set of entries at timestamp
     */
    Set<Map.Entry<K, V>> entrySet(TransactionId tid, 
            IsolationLevel isolationLevel, Filter filter, Comparator<V> comparator);

    /**
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param filter the filter
     * @return extant keys matching the filter
     */
    Set<K> keySet(TransactionId tid, 
            IsolationLevel isolationLevel, Filter filter);

    /**
     * Delete the index identified by the extractor.
     * @param extractor the extractor
     */
    void removeIndex(ValueExtractor extractor);

    /**
     * Perform an aggregation over a collection of keys as at the timestamp.
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param collKeys collection of keys
     * @param agent aggregator
     * @return the result of the aggregation
     * @param <R> result type of the aggregation
     */
    <R> R aggregate(TransactionId tid, 
            IsolationLevel isolationLevel, Collection<K> collKeys, 
            EntryAggregator agent);

    /**
     * Perform an aggregation against a filter as at the timestamp.
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param filter the filter
     * @param agent aggregator
     * @return the result of the aggregation
     * @param <R> result type of the aggregation
     */
    <R> R aggregate(TransactionId tid, 
            IsolationLevel isolationLevel, Filter filter, EntryAggregator agent);

    /**
     * Invoke an EntryProcessor against a collection of keys.
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param autoCommit implicit commit if true
     * @param readonly invoke as read only
     * @param collKeys collection of keys
     * @param agent the EntryProcessor
     * @return a map of key, result pairs, with the set of keys changed
     * @param <R> result type of the entryprocessor
     */
    <R> InvocationFinalResult<K, R> invokeAll(TransactionId tid, 
            IsolationLevel isolationLevel, boolean autoCommit, boolean readonly, Collection<K> collKeys, 
            EntryProcessor agent);

    /**
     * Invoke an EntryProcessor against a filter.
     * @param tid transaction id
     * @param isolationLevel isolation level
     * @param autoCommit implicit commit if true
     * @param readonly invoke as read only
     * @param filter the filter
     * @param agent the EntryProcessor
     * @return a map of key, result pairs, with the set of keys changed
     * @param <R> result type of the entryprocessor
     */
    <R> InvocationFinalResult<K, R> invokeAll(TransactionId tid, 
            IsolationLevel isolationLevel, boolean autoCommit, boolean readonly, Filter filter, EntryProcessor agent);

    /**
     * Destroy the cache.
     */
    void destroy();

    /**
     * @return the logical cache name
     */
    String getCacheName();

    /**
     * @return the Coherence service implementing the physical caches
     */
    CacheService getCacheService();

    /**
     * @return true if this cache is active.
     */
    boolean isActive();

    /**
     * release resources associated with this cache.
     */
    void release();

    /**
     * @return the {@link CacheName} object encapsulating logical cache name and physical key and version cache names.
     */
    CacheName getMVCCCacheName();

}