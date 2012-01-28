package com.sixwhits.cohmvcc.domain;

import java.io.Serializable;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.cache.KeyAssociation;

@Portable
public class VersionedKey<K> implements KeyAssociation, Serializable {
	
	private static final long serialVersionUID = 8459004703379236862L;
	
	public static final int POF_KEY = 0;
	@PortableProperty(POF_KEY)
	private K nativeKey;
	
	public static final int POF_TX = 1;
	@PortableProperty(POF_TX)
	private TransactionId txTimeStamp;
	
	public VersionedKey() {
	}
	
	public VersionedKey(K nativeKey, TransactionId txTimeStamp) {
		super();
		this.nativeKey = nativeKey;
		this.txTimeStamp = txTimeStamp;
	}

	public K getNativeKey() {
		return nativeKey;
	}

	public TransactionId getTxTimeStamp() {
		return txTimeStamp;
	}

	public Object getAssociatedKey() {
		if (nativeKey instanceof KeyAssociation) {
			return ((KeyAssociation)nativeKey).getAssociatedKey();
		} else {
			return nativeKey.hashCode();
		}
	}

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((nativeKey == null) ? 0 : nativeKey.hashCode());
        result = prime * result
                + ((txTimeStamp == null) ? 0 : txTimeStamp.hashCode());
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
        @SuppressWarnings({"rawtypes" })
        VersionedKey other = (VersionedKey) obj;
        if (nativeKey == null) {
            if (other.nativeKey != null) {
                return false;
            }
        } else if (!nativeKey.equals(other.nativeKey)) {
            return false;
        }
        if (txTimeStamp == null) {
            if (other.txTimeStamp != null) {
                return false;
            }
        } else if (!txTimeStamp.equals(other.txTimeStamp)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return nativeKey + " ...@" + txTimeStamp;
    }
	
	
}
