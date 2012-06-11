package com.shadowmvcc.coherence.invocable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.shadowmvcc.coherence.cache.CacheName;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Cluster;
import com.tangosol.net.Invocable;
import com.tangosol.net.InvocationObserver;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.PartitionedService;
import com.tangosol.net.Service;
import com.tangosol.net.partition.PartitionSet;

/**
 * 
 * Helper class to encapsulate the process of generating and sending invocables to manipulate
 * sets of keys or partitions one member at a time. Takes care of working out
 * which partitions to send to, retrying invocations when members leave and tracking that all
 * updates have completed.
 * 
 * This class is stateful and may not be completely clean after use so must be used on a "one-shot" basis.
 * Create an instance, perform one or more invokeAction* methods, then call waitForAllInvocations. Do not re-use.
 * 
 * @param <R> result type of invocations
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class InvocationServiceHelper<R> {
    
    /**
     * Interface for user-supplied factories for creating instances of Invocable.
     * 
     * @param <P> the type of target for an invocation instance. e.g. PartitionSet or key set
     */
    public interface InvocableFactory<P> {
        /**
         * Get an instance of Invocable for the specified target.
         * @param invocationTargetSet the target of this invocable
         * @return the invocable
         */
        Invocable getInvocable(P invocationTargetSet);
    }

    private final String invocationServiceName;
    private final Set<InvocationObserver> outstanding = new HashSet<InvocationObserver>(); 
    private final BlockingQueue<InvocationObserverStatus<?, R>> observerResultQueue = 
            new LinkedBlockingQueue<InvocationObserverStatus<?, R>>();

    /**
     * Constructor.
     * @param invocationServiceName name of the invocation service to use
     */
    public InvocationServiceHelper(final String invocationServiceName) {
        super();
        this.invocationServiceName = invocationServiceName;
    }
    
    /**
     * Static access wrapper to enable subclassing for unit testing.
     * @param serviceName the service name
     * @return the service
     */
    protected Service getService(final String serviceName) {
        return CacheFactory.getService(serviceName);
    }
    
    /**
     * Static access wrapper to enable subclassing for unit testing.
     * @param cacheName cache name
     * @return the NamedCache
     */
    protected NamedCache getCache(final String cacheName) {
        return CacheFactory.getCache(cacheName);
    }
    
    /**
     * Static access wrapper to enable subclassing for unit testing.
     * @return the cluster object
     */
    protected Cluster getCluster() {
        return CacheFactory.getCluster();
    }

    /**
     * Invoke the action for a set of keys on a cache. The keys are split by owning member
     * and a separate invocable sent to each member that owns any of the keys
     * @param cacheName cache name
     * @param keyset set of keys to invoke on
     * @param invocableFactory factory for creating invocable instances
     * @param <K> type of key
     */
    public <K> void invokeActionForKeyset(final CacheName cacheName,
            final Set<K> keyset, final InvocableFactory<Set<K>> invocableFactory) {
        
        InvocationService invocationService = (InvocationService) getService(invocationServiceName);

        PartitionedService cacheService =
                (PartitionedService) getCache(
                        cacheName.getVersionCacheName()).getCacheService();
        Map<Member, Set<K>> memberKeyMap = new HashMap<Member, Set<K>>();

        for (K key : keyset) {
            Member member = cacheService.getKeyOwner(key); 
            if (!memberKeyMap.containsKey(member)) {
                memberKeyMap.put(member, new HashSet<K>());
            }
            memberKeyMap.get(member).add(key);
        }
        
        for (Map.Entry<Member, Set<K>> memberKeyEntry : memberKeyMap.entrySet()) {
            Invocable invocable = invocableFactory.getInvocable(memberKeyEntry.getValue());
            KeyInvocationObserver<K, R> observer = new KeyInvocationObserver<K, R>(
                    memberKeyEntry.getValue(), cacheName, observerResultQueue, invocableFactory);
            outstanding.add(observer);
            invocationService.execute(invocable, Collections.singleton(memberKeyEntry.getKey()), observer);
        }
    }

    /**
     * Invoke the action for a set of partitions on a cache. The partitions are split by
     * member and a separate invocable sent to each member.
     * @param partitionSet partitions to process
     * @param cacheName cache name
     * @param invocableFactory factory for invocable instances
     */
    @SuppressWarnings("unchecked")
    public void invokeActionForPartitionSet(
            final PartitionSet partitionSet,
            final CacheName cacheName,
            final InvocableFactory<PartitionSet> invocableFactory) {
        
        InvocationService invocationService = (InvocationService) getService(invocationServiceName);
        
        PartitionedService cacheService =
                (PartitionedService) getCache(
                        cacheName.getVersionCacheName()).getCacheService();

        while (!partitionSet.isEmpty()) {
            for (Member member : (Set<Member>) getCluster().getMemberSet()) {
                PartitionSet memberPartitions = cacheService.getOwnedPartitions(member);
                memberPartitions.retain(partitionSet);
                if (!memberPartitions.isEmpty()) {
                    partitionSet.remove(memberPartitions);
                    Invocable invocable = invocableFactory.getInvocable(memberPartitions);
                    PartitionInvocationObserver<R> observer = new PartitionInvocationObserver<R>(
                            memberPartitions, cacheName, observerResultQueue, invocableFactory);
                    outstanding.add(observer);
                    invocationService.execute(invocable, Collections.singleton(member), observer);
                }
            }            
        }
    }
    
    /**
     * Re-invoke actions for this failed observer.
     * @param observer observer to re-invoke for
     * @param <K> key type 
     */
    private <K> void invokeActionForKeyObserver(final KeyInvocationObserver<K, R> observer) {
        invokeActionForKeyset(observer.getCachename(), observer.getInvocationTarget(), observer.getInvocableFactory());
    }
    
    /**
     * Wait for all invocations to complete. Retry any that failed because of member
     * departure
     * @return collection of result objects from the invocations
     * @throws Throwable if an invocation throws an exception
     */
    @SuppressWarnings("unchecked")
    public Collection<R> waitForAllInvocations() throws Throwable {
        
        List<R> result = new ArrayList<R>(outstanding.size());
        
        while (!outstanding.isEmpty()) {
            
            InvocationObserverStatus<?, R> observer = observerResultQueue.take();
            
            outstanding.remove(observer);
            
            if (observer.isFailed()) {
                if (observer.getFailureCause() != null) {
                    throw observer.getFailureCause();
                }
                
                if (observer instanceof KeyInvocationObserver) {
                    
                    invokeActionForKeyObserver((KeyInvocationObserver<?, R>) observer);
                    
                } else if (observer instanceof PartitionInvocationObserver) {
                    
                    PartitionInvocationObserver<R> partitionSetObserver = (PartitionInvocationObserver<R>) observer;
                    invokeActionForPartitionSet(partitionSetObserver.getInvocationTarget(),
                            partitionSetObserver.getCachename(), partitionSetObserver.getInvocableFactory());
                    
                }
            } else {
                result.add(observer.getResult());
            }
        }
        
        return result;
    }
}
