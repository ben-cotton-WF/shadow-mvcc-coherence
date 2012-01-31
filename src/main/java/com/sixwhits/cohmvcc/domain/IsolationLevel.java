package com.sixwhits.cohmvcc.domain;

public enum IsolationLevel {
	readUncommitted,
	readCommitted,
	repeatableRead,
	serializable
}
