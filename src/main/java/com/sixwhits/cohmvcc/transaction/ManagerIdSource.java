package com.sixwhits.cohmvcc.transaction;

/**
 * Defines a source of integer transaction manager ids. Each transaction manager
 * instance must be unique within the cluster at
 * any given time.
 *
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public interface ManagerIdSource {
    /**
     * @return a new, unique transaction manager id
     */
    int getManagerId();
}
