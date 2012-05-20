package com.shadowmvcc.coherence.utils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * utility methods for collections.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public final class MapUtils {
    
    /**
     * Empty constructor to prevent instantiation.
     */
    private MapUtils() {
    }

    /**
     * Merge a map of sets into another map of sets.
     * @param merginto map to merge into
     * @param mergefrom map to merge from
     * @param <C> value type of the set
     * @param <K> key type of the map
     */
    public static <C, K> void mergeSets(final Map<K, Set<C>> merginto, final Map<K, Set<C>> mergefrom) {
        if (mergefrom != null) {
            for (Map.Entry<K, Set<C>> ckEntry : mergefrom.entrySet()) {
                if (!merginto.containsKey(ckEntry.getKey())) {
                    merginto.put(ckEntry.getKey(), new HashSet<C>());
                }
                merginto.get(ckEntry.getKey()).addAll(ckEntry.getValue());
            }
        }
        
    }

}
