package com.sixwhits.cohmvcc.domain;

import java.io.Serializable;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

@Portable
public class TransactionalValue<V> implements Serializable {
	
	private static final long serialVersionUID = -51935479594043900L;
	
	public static final int POF_STATUS = 0;
	@PortableProperty(POF_STATUS)
	private TransactionStatus status;

	public static final int POF_VALUE = 1;
	@PortableProperty(POF_VALUE)
    private V value;
    
    public TransactionalValue() {
		super();
	}

	public TransactionalValue(TransactionStatus status, V value) {
        super();
        this.status = status;
        this.value = value;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public V getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        @SuppressWarnings("rawtypes")
		TransactionalValue other = (TransactionalValue) obj;
        if (status != other.status) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return value + "...[" + status + "]";
    }

}
