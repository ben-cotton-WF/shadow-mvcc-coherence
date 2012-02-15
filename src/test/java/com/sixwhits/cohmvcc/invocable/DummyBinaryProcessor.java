package com.sixwhits.cohmvcc.invocable;

import com.sixwhits.cohmvcc.domain.TransactionalValue;
import com.tangosol.io.ReadBuffer;
import com.tangosol.io.pof.PofContext;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.reflect.AbstractPofValue;
import com.tangosol.io.pof.reflect.PofNavigator;
import com.tangosol.io.pof.reflect.PofValue;
import com.tangosol.io.pof.reflect.PofValueParser;
import com.tangosol.io.pof.reflect.SimplePofPath;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.ValueExtractor;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.extractor.AbstractExtractor;
import com.tangosol.util.extractor.PofExtractor;
import com.tangosol.util.processor.AbstractProcessor;

@Portable
public class DummyBinaryProcessor extends AbstractProcessor {

	@Override
	public Object process(Entry arg0) {
		BinaryEntry entry = (BinaryEntry) arg0;
		
		Binary binCompleteVal = entry.getOriginalBinaryValue();
		
		PofExtractor versionValueExtractor = new PofExtractor(null, new SimplePofPath(TransactionalValue.POF_VALUE), AbstractExtractor.VALUE);
		
		Binary binLogicalVal2 = (Binary) versionValueExtractor.extractFromEntry(entry);
		
		PofNavigator nav = new SimplePofPath(TransactionalValue.POF_VALUE);
		
		Object result = entry.getBackingMapContext().getManagerContext().getValueFromInternalConverter().convert(binLogicalVal2);

		return result;
	}

}
