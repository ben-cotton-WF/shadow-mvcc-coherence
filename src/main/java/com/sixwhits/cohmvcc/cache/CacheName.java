package com.sixwhits.cohmvcc.cache;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

@Portable
public class CacheName {
	
	private static final String VERSION_CACHE_SUFFIX = "-version";
	private static final String KEY_CACHE_SUFFIX = "-key";
	private static final int POF_NAME = 0;
	@PortableProperty(POF_NAME)
	private String logicalName;
	transient private String versionCacheName = null;
	transient private String keyCacheName = null;
		
	public CacheName() {
		super();
	}

	public CacheName(String logicalName) {
		super();
		this.logicalName = logicalName;
	}

	public String getLogicalName() {
		return logicalName;
	}

	public String getVersionCacheName() {
		if (versionCacheName == null) {
			versionCacheName = logicalName + VERSION_CACHE_SUFFIX;
		}
		return versionCacheName;
	}

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
	public boolean equals(Object obj) {
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
