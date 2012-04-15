package com.shadowmvcc.coherence.pof;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.reflect.Codec;

/**
 * Codec to ensure a collection is instantiated as a {@code Set} on deserialisation.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SetCodec implements Codec {

    @SuppressWarnings("rawtypes")
    @Override
    public Object decode(final PofReader pofreader, final int i) throws IOException {
        return pofreader.readCollection(i, new HashSet());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void encode(final PofWriter pofwriter, final int i, final Object obj)
            throws IOException {
        pofwriter.writeCollection(i, (Collection) obj);
    }

}
