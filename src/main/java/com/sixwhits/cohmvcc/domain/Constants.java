package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;

/**
 * Constants for the domain package.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public final class Constants {

    /**
     * private constructor to prevent instantiation.
     */
    private Constants() {
    }

    public static final PofExtractor KEYEXTRACTOR = new PofExtractor(
            null, new SimplePofPath(VersionedKey.POF_KEY), AbstractExtractor.KEY);
    public static final PofExtractor TXEXTRACTOR = new PofExtractor(
            null, new SimplePofPath(VersionedKey.POF_TX), AbstractExtractor.KEY);
    public static final int DECO_COMMIT = ExternalizableHelper.DECO_APP_1;
    public static final int DECO_DELETED = ExternalizableHelper.DECO_APP_2;

}
