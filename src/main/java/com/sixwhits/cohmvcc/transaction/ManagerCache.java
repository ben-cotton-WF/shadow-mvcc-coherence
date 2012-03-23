package com.sixwhits.cohmvcc.transaction;


/**
 * Defines a source of integer transaction manager ids. Each transaction manager
 * instance must be unique within the cluster at
 * any given time.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface ManagerCache {
    /**
     * @return a new, unique transaction manager id
     */
    int getManagerId();
    
    /**
     * Register an MVCC cache so that client cleanup is aware of it.
     * @param id id of the manager
     * @param cacheName the logical name of the MVCC cache
     */
    void registerCache(int id, String cacheName);
}
