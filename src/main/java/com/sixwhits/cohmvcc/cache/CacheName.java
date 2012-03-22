package com.sixwhits.cohmvcc.cache;

import java.io.Serializable;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Class to encapsulate a logical cache name, and the physical version and key cache names. Use
 * the logical name to construct an instance, which can then be used to obtain the names of the
 * physical key and version caches
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class CacheName implements Serializable {

    private static final long serialVersionUID = -8922424456494883817L;
    private static final String VERSION_CACHE_SUFFIX = "-version";
    private static final String KEY_CACHE_SUFFIX = "-key";
    private static final int POF_NAME = 0;
    @PortableProperty(POF_NAME)
    private String logicalName;
    private transient String versionCacheName = null;
    private transient String keyCacheName = null;

    /**
     * Default constructor for POF use only.
     */
    public CacheName() {
        super();
    }

    /**
     * Constructor.
     * @param logicalName logical name of the cache
     */
    public CacheName(final String logicalName) {
        super();
        this.logicalName = logicalName;
    }

    /**
     * Get the cache logical name.
     * @return the cache logical name
     */
    public String getLogicalName() {
        return logicalName;
    }

    /**
     * Get the name of the physical version cache.
     * @return the version cache name
     */
    public String getVersionCacheName() {
        if (versionCacheName == null) {
            versionCacheName = logicalName + VERSION_CACHE_SUFFIX;
        }
        return versionCacheName;
    }

    /**
     * Get the name of the physical key cache.
     * @return the key cache name
     */
    public String getKeyCacheName() {
        if (keyCacheName == null) {
            keyCacheName = logicalName + KEY_CACHE_SUFFIX;
        }
        return keyCacheName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((logicalName == null) ? 0 : logicalName.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CacheName other = (CacheName) obj;
        if (logicalName == null) {
            if (other.logicalName != null) {
                return false;
            }
        } else if (!logicalName.equals(other.logicalName)) {
            return false;
        }
        return true;
    }

}
