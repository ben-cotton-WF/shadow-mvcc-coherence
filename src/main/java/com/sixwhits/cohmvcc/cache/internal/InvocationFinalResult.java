package com.sixwhits.cohmvcc.cache.internal;

import java.util.Map;
import java.util.Set;

/**
 * Result type for filter based ntryProcessor invocations. Encapsulates the
 * map of key/result pairs for the entryprocessor invocations and a set of keys that were
 * changed. The changed keys may contain entries not in the map if the EntryProcessor processAll()
 * method did not return them. It may also omit keys in the result map where these cache
 * entries were not modified by the EntrProcessor.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> key type of the cache
 * @param <R> the result type of the EntryProcessor process method
 */
public class InvocationFinalResult<K, R> {
    private final Map<K, R> resultMap;
    private final Set<K> changedKeys;
    
    /** Constructor.
     * @param resultMap map of EntryProcessor results
     * @param changedKeys set of keys changed by the invocation
     */
    public InvocationFinalResult(final Map<K, R> resultMap, final Set<K> changedKeys) {
        super();
        this.resultMap = resultMap;
        this.changedKeys = changedKeys;
    }
    /**
     * Get the map of processor results.
     * @return the map of processor results
     */
    public Map<K, R> getResultMap() {
        return resultMap;
    }
    /**
     * Get the set of keys changed by the invocation.
     * @return the set of changed keys
     */
    public Set<K> getChangedKeys() {
        return changedKeys;
    }
    
}