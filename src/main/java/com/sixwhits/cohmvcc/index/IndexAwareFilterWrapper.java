package com.sixwhits.cohmvcc.index;

import java.util.Map;
import java.util.Set;

import com.tangosol.util.Filter;
import com.tangosol.util.filter.IndexAwareFilter;

public class IndexAwareFilterWrapper extends FilterWrapper implements IndexAwareFilter {
	
	public IndexAwareFilterWrapper(IndexAwareFilter delegate) {
		super(delegate);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int calculateEffectiveness(Map map, Set set) {
		return ((IndexAwareFilter) delegate).calculateEffectiveness(map, set);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Filter applyIndex(Map map, Set set) {
		return ((IndexAwareFilter) delegate).applyIndex(map, set);
	}


}
