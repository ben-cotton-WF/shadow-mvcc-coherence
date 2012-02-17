package com.sixwhits.cohmvcc.domain;

public enum IsolationLevel {
	readProhibited,
	readUncommitted,
	readCommitted,
	repeatableRead,
	serializable
}
