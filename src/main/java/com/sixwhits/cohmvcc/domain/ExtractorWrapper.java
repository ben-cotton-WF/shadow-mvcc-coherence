package com.sixwhits.cohmvcc.domain;

import java.util.Map.Entry;

import com.sixwhits.cohmvcc.invocable.VersionCacheBinaryEntryWrapper;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.extractor.AbstractExtractor;

/**
 * Wrapper for a {@link ValueExtractor} so that it sees the cache entry
 * as the logical cache view.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class ExtractorWrapper extends AbstractExtractor {

    private static final long serialVersionUID = 6157949928177567708L;

    public static final int POF_EXTRACTOR = 0;
    @PortableProperty(POF_EXTRACTOR)
    private ValueExtractor delegate;

    /**
     * Default constructor for POF use only.
     */
    public ExtractorWrapper() {
        super();
    }

    /**
     * Constructor.
     * @param delegate teh extractor to delegate to
     */
    public ExtractorWrapper(final ValueExtractor delegate) {
        super();
        this.delegate = delegate;
    }

    @Override
    public Object extractFromEntry(@SuppressWarnings("rawtypes") final Entry entry) {

        BinaryEntry wrapper = new VersionCacheBinaryEntryWrapper((BinaryEntry) entry);

        if (delegate instanceof AbstractExtractor) {
            return (((AbstractExtractor) delegate).extractFromEntry(wrapper));
        } else {
            return delegate.extract(wrapper.getValue());
        }

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((delegate == null) ? 0 : delegate.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ExtractorWrapper other = (ExtractorWrapper) obj;
        if (delegate == null) {
            if (other.delegate != null) {
                return false;
            }
        } else if (!delegate.equals(other.delegate)) {
            return false;
        }
        return true;
    }

}
