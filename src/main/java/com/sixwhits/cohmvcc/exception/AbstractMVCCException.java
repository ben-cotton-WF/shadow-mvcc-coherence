package com.sixwhits.cohmvcc.exception;

import java.io.IOException;

import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableException;

public abstract class AbstractMVCCException extends PortableException {
	
	private static final long serialVersionUID = 2338727005397469974L;
	private VersionedKey<?> key;
	
	public VersionedKey<?> getKey() {
		return key;
	}

	public AbstractMVCCException() {
		super();
	}
	
	public AbstractMVCCException(VersionedKey<?> key) {
		super();
		this.key = key;
	}

	public AbstractMVCCException(VersionedKey<?> key, String message) {
		super(message);
		this.key = key;
	}

	@Override
	public void readExternal(PofReader in) throws IOException {
		super.readExternal(in);
		key = (VersionedKey<?>) in.readObject(1000);
	}

	@Override
	public void writeExternal(PofWriter out) throws IOException {
		super.writeExternal(out);
		out.writeObject(1000, key);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
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
		AbstractMVCCException other = (AbstractMVCCException) obj;
		if (key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!key.equals(other.key)) {
			return false;
		}
		return true;
	}


}
