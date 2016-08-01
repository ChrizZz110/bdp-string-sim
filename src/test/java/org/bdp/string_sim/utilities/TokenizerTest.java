package org.bdp.string_sim.utilities;

import java.util.ArrayList;
import java.util.Arrays;

import junit.framework.TestCase;

public class TokenizerTest extends TestCase {
    
    public void setUp() throws Exception {
        super.setUp();
    }
    
    public void testTokenizer()throws Exception {
    	ArrayList<String> tokenizeAssert = new ArrayList<String>(Arrays.asList("##T", "#To", "Tok", "oke", "ken", "eni", "niz", "ize", "zer", "er#", "r##"));
    	ArrayList<String> testData = new ArrayList<String>(Arrays.asList("Tokenizer", "Tok", ""));
    	// default token size 3
    	Tokenizer tokenizerDefault = new Tokenizer();
    	Tokenizer tokenizerCustom = new Tokenizer(1);
    	
    	// test correct number of tokens
    	assertTrue(tokenizerDefault.tokenize(testData.get(0)).size() == 11);
    	assertTrue(tokenizerDefault.tokenize(testData.get(1)).size() == 5);
    	assertTrue(tokenizerDefault.tokenize(testData.get(2)).size() == 0);
    	assertTrue(tokenizerCustom.tokenize(testData.get(0)).size() == 9);
    	assertTrue(tokenizerCustom.tokenize(testData.get(1)).size() == 3);
    	assertTrue(tokenizerCustom.tokenize(testData.get(2)).size() == 0);
    	
    	// test tokenization
    	ArrayList<String> tokenizeResult = tokenizerDefault.tokenize(testData.get(0));
    	assertTrue(tokenizeResult.containsAll(tokenizeAssert));
    }
}
