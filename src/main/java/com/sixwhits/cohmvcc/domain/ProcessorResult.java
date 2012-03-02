package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

@Portable
public class ProcessorResult<K,R> {
	
	public static final int POF_RESULT = 0;
	@PortableProperty(POF_RESULT)
	private R result;
	public static final int POF_WAITKEY = 1;
	@PortableProperty(POF_WAITKEY)
	private VersionedKey<K> waitKey;
	
	public ProcessorResult() {
		super();
	}

	public ProcessorResult(R result, VersionedKey<K> waitKey) {
		super();
		this.result = result;
		this.waitKey = waitKey;
	}
	
	public R getResult() {
		return result;
	}

	public void setResult(R result) {
		this.result = result;
	}

	public boolean isUncommitted() {
		return waitKey != null;
	}

	public VersionedKey<K> getWaitKey() {
		return waitKey;
	}

	public void setWaitKey(VersionedKey<K> waitKey) {
		this.waitKey = waitKey;
	}

}
