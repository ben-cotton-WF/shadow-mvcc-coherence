package com.sixwhits.cohmvcc.invocable;

import java.util.Map;
import java.util.Set;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.processor.Reducer;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Filter;
import com.tangosol.util.InvocableMap.EntryProcessor;

/**
 * Abstract processor for delegating to a logical view processor.
 * If the delegate is an instance of {@link Reducer}, then will call
 * the reduce method
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> key type
 * @param <R> processor result type
 */
@Portable
public abstract class AbstractMVCCProcessorWrapper<K, R> extends AbstractMVCCProcessor<K, R> {

    private static final long serialVersionUID = 8572836706068655491L;
    
    @PortableProperty(10) protected EntryProcessor delegate;

    /**
     *  Default constructor for POF use only.
     */
    public AbstractMVCCProcessorWrapper() {
        super();
    }

    /**
     * Constructor.
     * @param transactionId current transaction id
     * @param isolationLevel current isolation level
     * @param cacheName cache name
     * @param delegate EntryProcessor to execute
     */
    public AbstractMVCCProcessorWrapper(final TransactionId transactionId,
            final IsolationLevel isolationLevel, final CacheName cacheName,
            final EntryProcessor delegate) {
        super(transactionId, isolationLevel, cacheName);
        this.delegate = delegate;
    }

    /**
     * Constructor.
     * @param transactionId current transaction id
     * @param isolationLevel current isolation level
     * @param cacheName cache name
     * @param validationFilter filter to confirm whether to process
     * @param delegate EntryProcessor to execute
     */
    public AbstractMVCCProcessorWrapper(final TransactionId transactionId,
            final IsolationLevel isolationLevel, final CacheName cacheName,
            final Filter validationFilter, final EntryProcessor delegate) {
        super(transactionId, isolationLevel, cacheName, validationFilter);
        this.delegate = delegate;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map processAll(final Set set) {

        Map result = super.processAll(set);
        
        if (delegate instanceof Reducer) {
            result = ((Reducer) delegate).reduce(result);
        }
        
        return result;

    }


}
