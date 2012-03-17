package com.sixwhits.cohmvcc.cache.internal;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Member;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class LongWaitingEntryProcessor extends AbstractProcessor {

    private static final long serialVersionUID = -4126192997745434280L;

    @Override
    public Object process(Entry entry) {
        Member thisMember = CacheFactory.getCluster().getLocalMember();
        if (thisMember.getId() == 1) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
        return thisMember.getId();
    }

}
