package org.bdp.string_sim.utilities;

import java.util.ArrayList;

public class Tokenizer {

	private int nGramDigits = 3;

	public Tokenizer() {
	}
	
	/**
     * @param nGramDigits size of tokens that are generated
     */
	public Tokenizer(int nGramDigits) {
		this.nGramDigits = nGramDigits;
	}
	
	/**
     * This Function separates a String, based on the value in nGramDigits, into a number of Strings (Tokens)
     * @param value of type String that will be tokenized
     * @return ArrayList of tokens
     * @throws Exception
     */
	public ArrayList<String> tokenize(String value) throws Exception {
		ArrayList<String> tokens = new ArrayList<String>();
		
		if (!value.isEmpty()) {
			//generate placeholder characters around value, according to property nGramDigits
			String placeholder = (new String(new char[nGramDigits-1]).replace('\0','#'));
			String placeholderValue = placeholder + value + placeholder;
			
			//tokenize
			for (int i=0; i<placeholderValue.length()+1-nGramDigits; i++) {
				tokens.add(placeholderValue.substring(i, nGramDigits + i));
			}
		}
		return tokens;
	}
}