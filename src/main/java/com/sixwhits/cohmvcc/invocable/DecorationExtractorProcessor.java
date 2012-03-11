package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.Constants;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class DecorationExtractorProcessor extends AbstractProcessor {

	private static final long serialVersionUID = 5931592896780862265L;
	
	@PortableProperty(0)
	private int decoId;
	
	public static final DecorationExtractorProcessor COMMITTED_INSTANCE = new DecorationExtractorProcessor(Constants.DECO_COMMIT);
	public static final DecorationExtractorProcessor DELETED_INSTANCE = new DecorationExtractorProcessor(Constants.DECO_DELETED);
	
	public DecorationExtractorProcessor() {
		super();
	}

	public DecorationExtractorProcessor(int decoId) {
		super();
		this.decoId = decoId;
	}

	@Override
	public Object process(Entry entry) {
		BinaryEntry binEntry = (BinaryEntry) entry;
		Binary binValue = binEntry.getBinaryValue();
		if (ExternalizableHelper.isDecorated(binValue)) {
			Binary binDeco = ExternalizableHelper.getDecoration(binValue, decoId);
			if (binDeco != null) {
				return ExternalizableHelper.fromBinary(binDeco, binEntry.getSerializer());
			}
		}
		
		return null;
	}

}
