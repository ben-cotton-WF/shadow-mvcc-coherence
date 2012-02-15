package com.sixwhits.cohmvcc.domain;

import java.io.Serializable;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;

@Portable
public class TransactionalValue implements Serializable {
	
	private static final long serialVersionUID = -51935479594043900L;
	
	public static final int POF_COMMITTED = 0;
	@PortableProperty(POF_COMMITTED)
	private boolean committed;
	public static final int POF_DELETED = 1;
	@PortableProperty(POF_DELETED)
	private boolean deleted;

	public static final int POF_VALUE = 2;
	@PortableProperty(POF_VALUE)
    private Binary value;
    
    public TransactionalValue() {
		super();
	}

	public TransactionalValue(boolean committed, boolean deleted, Binary value) {
        super();
        this.committed = committed;
        this.deleted = deleted;
        this.value = (committed && deleted) ? null : value;
    }

    public boolean isCommitted() {
        return committed;
    }

    public boolean isDeleted() {
		return deleted;
	}

	public void setCommitted(boolean committed) {
		this.committed = committed;
		if (deleted) {
			value = null;
		}
	}

	public Binary getValue() {
        return value;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (committed ? 1231 : 1237);
		result = prime * result + (deleted ? 1231 : 1237);
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionalValue other = (TransactionalValue) obj;
		if (committed != other.committed)
			return false;
		if (deleted != other.deleted)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return  (deleted ? "[deleted]" : value) + (committed ? "" : "[uncommitted]");
	}


}
