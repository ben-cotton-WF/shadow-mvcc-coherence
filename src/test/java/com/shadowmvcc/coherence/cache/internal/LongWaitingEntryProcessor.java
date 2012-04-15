package com.shadowmvcc.coherence.cache.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Member;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Test support class that just takes a long time to process.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class LongWaitingEntryProcessor extends AbstractProcessor {

    private static final long serialVersionUID = -4126192997745434280L;

    @Override
    public Object process(final Entry entry) {
        Member thisMember = CacheFactory.getCluster().getLocalMember();
        if (thisMember.getId() == 1) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
        return thisMember.getId();
    }

}
