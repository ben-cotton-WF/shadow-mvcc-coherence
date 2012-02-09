package com.sixwhits.cohmvcc.pof;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.reflect.Codec;

public class SortedSetCodec implements Codec {

	@SuppressWarnings("rawtypes")
	@Override
	public Object decode(PofReader pofreader, int i) throws IOException {
		return pofreader.readCollection(i, new TreeSet());
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void encode(PofWriter pofwriter, int i, Object obj)
			throws IOException {
		pofwriter.writeCollection(i, (Collection)obj);
	}

}
