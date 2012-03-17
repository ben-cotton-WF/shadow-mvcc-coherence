package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Encapsulate the result of a single {@code EntryProcessor} invocation. This
 * may be the actual return value of a wrapped {@code EntryProcessor}, or the
 * version cache key of an uncommitted entry that prevented completion.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the cache logical key type
 * @param <R> the wrapped {@code EntryProcessor} result type
 */
@Portable
public class ProcessorResult<K, R> {

    public static final int POF_RESULT = 0;
    @PortableProperty(POF_RESULT)
    private R result;
    public static final int POF_WAITKEY = 1;
    @PortableProperty(POF_WAITKEY)
    private VersionedKey<K> waitKey;

    /**
     *  Default constructor for POF use only.
     */
    public ProcessorResult() {
        super();
    }

    /**
     * Constructor. Only one of the parameters should be non-null.
     * @param result the {@code EntryProcessor} result
     * @param waitKey the version cache key of an uncommitted entry
     */
    public ProcessorResult(final R result, final VersionedKey<K> waitKey) {
        super();
        this.result = result;
        this.waitKey = waitKey;
    }

    /**
     * @return the {@code EntryProcessor} result or null
     * if processing could not proceed because of an uncommitted entry
     */
    public R getResult() {
        return result;
    }

    /**
     * @return true if this result represents an uncommitted entry
     */
    public boolean isUncommitted() {
        return waitKey != null;
    }

    /**
     * @return the version cache key that prevented execution or null
     */
    public VersionedKey<K> getWaitKey() {
        return waitKey;
    }

}
