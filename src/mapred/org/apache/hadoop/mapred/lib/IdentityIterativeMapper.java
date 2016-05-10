package org.apache.hadoop.mapred.lib;

import java.io.IOException;

import org.apache.hadoop.mapred.IterativeMapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IdentityIterativeMapper<SK, SV, DK, DV, K2, V2> extends MapReduceBase implements
		IterativeMapper<SK, SV, DK, DV, K2, V2> {

	@Override
	public void map(SK statickey, SV staticval, DK dynamickey, DV dynamicvalue,
			OutputCollector<K2, V2> output, Reporter reporter)
			throws IOException {
		output.collect((K2)statickey, (V2)dynamicvalue);
	}

	@Override
	public V2 removeLable() {
		// TODO Auto-generated method stub
		return null;
	}

}
