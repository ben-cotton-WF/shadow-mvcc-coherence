package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.Serializer;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;

public class Utils {
	
	private Utils() {} // prevent instantiation
	
	private static boolean getBooleanDecoration(int decoId, BinaryEntry binEntry, boolean defaultValue) {
		boolean result = defaultValue;
		Binary binValue = binEntry.getBinaryValue();
		if (binValue != null) {
			if (ExternalizableHelper.isDecorated(binValue)) {
				Binary binDeco = ExternalizableHelper.getDecoration(binValue, decoId);
				if (binDeco != null) {
					result = (Boolean) ExternalizableHelper.fromBinary(binDeco, binEntry.getSerializer());
				}
			}
		}
		
		return result;
		
	}
	
	public static boolean isCommitted(BinaryEntry binaryEntry) {
		return getBooleanDecoration(Constants.DECO_COMMIT, binaryEntry, true);
	}
	
	public static boolean isDeleted(BinaryEntry binaryEntry) {
		return getBooleanDecoration(Constants.DECO_DELETED, binaryEntry, false);
	}

	public static Binary decorateValue(Binary binaryValue, boolean committed,
			boolean deleted, Serializer serializer) {
		
		Binary result = addBooleanDecoration(binaryValue, Constants.DECO_COMMIT, committed, serializer); 
		result = addBooleanDecoration(result, Constants.DECO_DELETED, deleted, serializer);
		
		return result;
		
	}

	private static Binary addBooleanDecoration(Binary binValue, int decoId,
			boolean decoValue, Serializer serializer) {
		Binary decoBin = ExternalizableHelper.toBinary(decoValue, serializer);
		return ExternalizableHelper.decorate(binValue, decoId, decoBin);
	}

	public static void setCommitted(BinaryEntry entry, boolean committed) {
		Binary binValue = entry.getBinaryValue();
		binValue = addBooleanDecoration(binValue, Constants.DECO_COMMIT, committed, entry.getSerializer());
		entry.updateBinaryValue(binValue);
	}

}
