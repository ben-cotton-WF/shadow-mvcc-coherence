package com.shadowmvcc.coherence.invocable;

import java.util.Queue;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.invocable.InvocationServiceHelper.InvocableFactory;
import com.tangosol.net.Member;

/**
 * Invocation observer and result status.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <P> type of the target of the invocation.
 * @param <R> type of result from the invocable
 */
public class InvocationObserverStatusImpl<P, R> implements InvocationObserverStatus<P, R> {

    protected final Queue<InvocationObserverStatus<?, R>> resultQueue;
    protected final InvocableFactory<P> invocableFactory;
    protected final CacheName cachename;
    protected final P invocationTarget;
    private volatile boolean failed = false;
    private volatile Throwable failureCause = null;
    private volatile R result = null;

    /**
     * Constructor.
     * @param invocationTarget invocation target
     * @param cachename cache name
     * @param resultQueue filter to place self on when complete
     * @param invocableFactory the invocable factory used to create the invocable being observed
     */
    public InvocationObserverStatusImpl(final P invocationTarget, final CacheName cachename,
            final Queue<InvocationObserverStatus<?, R>> resultQueue,
            final InvocableFactory<P> invocableFactory) {
        super();
        this.cachename = cachename;
        this.resultQueue = resultQueue;
        this.invocationTarget = invocationTarget;
        this.invocableFactory = invocableFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void memberCompleted(final Member member, final Object obj) {
        result = (R) obj;
    }

    @Override
    public void memberFailed(final Member member, final Throwable throwable) {
        failed = true;
        failureCause = throwable;
    }

    @Override
    public void memberLeft(final Member member) {
        failed = true;
    }

    @Override
    public void invocationCompleted() {
        resultQueue.add(this);
    }

    /**
     * @return the cache name
     */
    public CacheName getCachename() {
        return cachename;
    }

    @Override
    public boolean isFailed() {
        return failed;
    }

    @Override
    public P getInvocationTarget() {
        return invocationTarget;
    }

    @Override
    public Throwable getFailureCause() {
        return failureCause;
    }

    @Override
    public InvocableFactory<P> getInvocableFactory() {
        return invocableFactory;
    }

    @Override
    public R getResult() {
        return result;
    }

}
