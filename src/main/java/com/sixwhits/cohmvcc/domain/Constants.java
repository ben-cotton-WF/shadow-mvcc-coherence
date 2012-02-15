package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;

public class Constants {

	public static final PofExtractor VERSIONEXTRACTOR =      new PofExtractor(null, new SimplePofPath(VersionedKey.POF_TX), AbstractExtractor.KEY);
	public static final PofExtractor VALUEEXTRACTOR =        new PofExtractor(null, new SimplePofPath(TransactionalValue.POF_VALUE), AbstractExtractor.VALUE);
	public static final PofExtractor COMMITSTATUSEXTRACTOR = new PofExtractor(null, new SimplePofPath(TransactionalValue.POF_COMMITTED), AbstractExtractor.VALUE);
	public static final PofExtractor DELETESTATUSEXTRACTOR = new PofExtractor(null, new SimplePofPath(TransactionalValue.POF_DELETED), AbstractExtractor.VALUE);
	public static final PofExtractor KEYEXTRACTOR =          new PofExtractor(null, new SimplePofPath(VersionedKey.POF_KEY), AbstractExtractor.KEY);
	public static final PofExtractor TXEXTRACTOR =           new PofExtractor(null, new SimplePofPath(VersionedKey.POF_TX), AbstractExtractor.KEY);

}
