package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.annotation.Portable;

@Portable
public class DeletedObject {
	
	public static final DeletedObject INSTANCE = new DeletedObject();
	
	@Override
	public int hashCode() {
		return 31;
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
		return true;
	}

}
