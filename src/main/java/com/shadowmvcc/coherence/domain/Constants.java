package com.shadowmvcc.coherence.domain;

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

    /**
     * An instance of {@code PofExtractor} that may be used to obtain the logical key
     * from the {@link VersionedKey} in the version cache.
     */
    public static final PofExtractor KEYEXTRACTOR = new PofExtractor(
            null, new SimplePofPath(VersionedKey.POF_KEY), AbstractExtractor.KEY);
    /**
     * An instance of {@code PofExtractor} that may be used to obtain the {@code TransactionId}
     * from the {@link VersionedKey} in the version cache.
     */
    public static final PofExtractor TXEXTRACTOR = new PofExtractor(
            null, new SimplePofPath(VersionedKey.POF_TX), AbstractExtractor.KEY);
    /**
     * The decoration id used to store the commit status of a version cache entry.
     */
    public static final int DECO_COMMIT = ExternalizableHelper.DECO_APP_1;
    
    /**
     * The decoration id used to store the deleted status of a version cache entry.
     */
    public static final int DECO_DELETED = ExternalizableHelper.DECO_APP_2;

}
