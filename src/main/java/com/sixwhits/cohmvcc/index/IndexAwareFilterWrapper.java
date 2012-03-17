package com.sixwhits.cohmvcc.index;

import java.util.Map;
import java.util.Set;

import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;

/**
 * wrapper for index aware filter.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class IndexAwareFilterWrapper extends FilterWrapper implements IndexAwareFilter {

    /**
     * @param delegate wrapped filter
     */
    public IndexAwareFilterWrapper(final IndexAwareFilter delegate) {
        super(delegate);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int calculateEffectiveness(final Map map, final Set set) {
        return ((IndexAwareFilter) delegate).calculateEffectiveness(map, set);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Filter applyIndex(final Map map, final Set set) {
        return ((IndexAwareFilter) delegate).applyIndex(map, set);
    }


}
