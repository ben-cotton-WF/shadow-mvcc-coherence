package com.sixwhits.cohmvcc.exception;

import com.sixwhits.cohmvcc.domain.VersionedKey;

public class FutureReadException extends AbstractMVCCException {

	private static final long serialVersionUID = 7054631620027498791L;

	public FutureReadException(VersionedKey<?> key, String message) {
		super(key, message);
	}

	public FutureReadException(VersionedKey<?> key) {
		super(key);
	}
	
	public FutureReadException() {
		super();
	}


}
