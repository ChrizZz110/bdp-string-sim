package org.bdp.string_sim.process;

import java.util.ArrayList;

import org.apache.flink.util.Collector;

public class Tokenizer {

	public ArrayList<String> flatMap(String value) throws Exception {
		
		if (!value.isEmpty()) {
			ArrayList<String> arr = new ArrayList<String>();
			String fullValue = "##" + value + "##";
			
			for (int i=0; i<value.length()+2; i++){
				arr.add(fullValue.substring(0+i, 3+i));
			}
			return arr;
		}
		return null;
		
	}
	
	public void flatMap(String value, Collector<String> out, Integer nGram ) throws Exception {
		if (!value.isEmpty()) {
			String fullValue = "##" + value + "##";
			for (int i=0; i<value.length()+2 && value.length()>nGram; i++){
				out.collect(new String(fullValue.substring(0+i, 3+i+nGram)));
			}
		}
	}
}
