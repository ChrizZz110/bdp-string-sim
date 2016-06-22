package org.bdp.string_sim.process;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Tokenizer implements FlatMapFunction<String, String>{

	private int nGramDigits = 3;

	public Tokenizer() {
	}
	
	/**
     * @param nGramDigits size of tokens that are generated
     */
	public Tokenizer(int nGramDigits) {
		this.nGramDigits = nGramDigits;
		
		//TODO: may delimit to values that are plausible
	}
	
	/**
     * This Function separates a String, based on the value in nGramDigits, into a number of Strings (Tokens)
     * @param value of type String that will be tokenized
     * @throws Exception
     */
	public void flatMap(String value, Collector<String> out) throws Exception {
		
		if (!value.isEmpty()) {
			
			//generate placeholder characters around value, according to property nGramDigits
			String placeholderValue = 	(new String(new char[nGramDigits-1]).replace('\0','#')) +
										value +
										(new String(new char[nGramDigits-1]).replace('\0','#'));
			
			//tokenize
			for (int i=0; i<placeholderValue.length()+1-nGramDigits; i++) {
				out.collect(new String (placeholderValue.substring(0+i, nGramDigits+i)));
			}	
		}
	}
}