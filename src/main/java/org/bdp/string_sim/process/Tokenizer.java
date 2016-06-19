package org.bdp.string_sim.process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

public class Tokenizer implements FlatMapFunction<String,Tuple1<String>> {

	@Override
	public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
		if (!value.isEmpty()) {
			String fullValue = "##" + value + "##";
			for (int i=0; i<value.length()+2; i++){
				out.collect(new Tuple1<String>(fullValue.substring(0+i, 3+i)));
			}
		}
	}
}
