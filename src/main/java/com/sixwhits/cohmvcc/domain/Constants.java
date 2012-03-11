package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;

public class Constants {
	
	private Constants() {} //prevent instantiation

	public static final PofExtractor KEYEXTRACTOR =          new PofExtractor(null, new SimplePofPath(VersionedKey.POF_KEY), AbstractExtractor.KEY);
	public static final PofExtractor TXEXTRACTOR =           new PofExtractor(null, new SimplePofPath(VersionedKey.POF_TX), AbstractExtractor.KEY);
	public static final int DECO_COMMIT = ExternalizableHelper.DECO_APP_1;
	public static final int DECO_DELETED = ExternalizableHelper.DECO_APP_2;
	
}
