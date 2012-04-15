/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

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
