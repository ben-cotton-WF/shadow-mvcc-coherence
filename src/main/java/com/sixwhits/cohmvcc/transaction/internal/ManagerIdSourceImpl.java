package com.sixwhits.cohmvcc.transaction.internal;

import com.sixwhits.cohmvcc.pof.IdentityPofPath;
import com.sixwhits.cohmvcc.transaction.ManagerIdSource;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.ValueManipulator;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.extractor.CompositeUpdater;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.extractor.PofUpdater;
import com.tangosol.util.filter.NotFilter;
import com.tangosol.util.filter.PresentFilter;
import com.tangosol.util.processor.CompositeProcessor;
import com.tangosol.util.processor.ConditionalPut;
import com.tangosol.util.processor.NumberIncrementor;

/**
 * Implementation of {@link ManagerIdSource} that obtains ids from
 * a cache.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ManagerIdSourceImpl implements ManagerIdSource {

    private static final String CACHENAME = "transaction-manager-id";
    private static final int KEY = 0;
    private static final EntryProcessor GETMANAGERID;

    static {
        Filter filter = new NotFilter(PresentFilter.INSTANCE);
        EntryProcessor initialiseProcessor = new ConditionalPut(filter, Integer.valueOf(0));

        ValueExtractor numberExtractor = new PofExtractor(Integer.class, IdentityPofPath.INSTANCE);
        ValueUpdater  numberUpdater = new PofUpdater(IdentityPofPath.INSTANCE);
        ValueManipulator manip = new CompositeUpdater(numberExtractor, numberUpdater);
        EntryProcessor incrementProcessor = new NumberIncrementor(manip, 1, true);

        EntryProcessor[] procarray;
        procarray = new EntryProcessor[2];
        procarray[0] = initialiseProcessor;
        procarray[1] = incrementProcessor;

        GETMANAGERID = new CompositeProcessor(procarray);
    }

    @Override
    public int getManagerId() {

        NamedCache managerIdCache = CacheFactory.getCache(CACHENAME);

        return (Integer) managerIdCache.invoke(KEY, GETMANAGERID);

    }
}
