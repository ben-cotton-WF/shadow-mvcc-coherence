package com.shadowmvcc.coherence.transaction;

import java.util.GregorianCalendar;

/**
 * Extend a Gregorian Calendar so it can be used as a timestamp source for transactions.
 * Intended to support <strong>as at</strong> queries.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SettableTimestampSource extends GregorianCalendar implements
        TimestampSource {

    private static final long serialVersionUID = 3305725904964989345L;

    @Override
    public long getTimestamp() {
        return this.getTimeInMillis();
    }

}
