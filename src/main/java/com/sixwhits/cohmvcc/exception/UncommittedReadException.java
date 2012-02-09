package com.sixwhits.cohmvcc.exception;

import com.sixwhits.cohmvcc.domain.VersionedKey;

public class UncommittedReadException extends AbstractMVCCException {

	private static final long serialVersionUID = 7617017177058160058L;

	public UncommittedReadException(VersionedKey<?> key, String message) {
		super(key, message);
	}

	public UncommittedReadException(VersionedKey<?> key) {
		super(key);
	}

	public UncommittedReadException() {
		super();
	}


}
