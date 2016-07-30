package org.bdp.string_sim.utilities;

import junit.framework.TestCase;

import java.util.ArrayList;

public class DictionaryTest extends TestCase {

    private final ArrayList<String> tokenizedNGramList = new ArrayList<>();

    public void setUp() throws Exception{
        super.setUp();

        //fassnassn
        tokenizedNGramList.add("__f");
        tokenizedNGramList.add("_fa");
        tokenizedNGramList.add("fas");
        tokenizedNGramList.add("ass");
        tokenizedNGramList.add("ssn");
        tokenizedNGramList.add("sna");
        tokenizedNGramList.add("nas");
        tokenizedNGramList.add("ass");
        tokenizedNGramList.add("ssn");
        tokenizedNGramList.add("sn_");
        tokenizedNGramList.add("n__");
    }

    public void testAddToDictionary() throws Exception{
        Dictionary dictionary = new Dictionary();

        tokenizedNGramList.forEach(dictionary::add);

        for (String trigram : tokenizedNGramList) {
            assertTrue(dictionary.getDictionary().containsKey(trigram));
        }

        assertEquals(dictionary.getIndex(tokenizedNGramList.get(3)),dictionary.getIndex(tokenizedNGramList.get(7)));
        assertEquals(dictionary.getIndex(tokenizedNGramList.get(4)),dictionary.getIndex(tokenizedNGramList.get(8)));

    }

    public void testAddListToDictionary() throws Exception{
        Dictionary dictionary = new Dictionary();

        dictionary.add(tokenizedNGramList);

        for (String trigram : tokenizedNGramList) {
            assertTrue(dictionary.getDictionary().containsKey(trigram));
        }

        assertEquals(dictionary.getIndex(tokenizedNGramList.get(3)),dictionary.getIndex(tokenizedNGramList.get(7)));
        assertEquals(dictionary.getIndex(tokenizedNGramList.get(4)),dictionary.getIndex(tokenizedNGramList.get(8)));

    }

    public void testGetIndexListForNGrams() throws Exception{
        Dictionary dictionary = new Dictionary();

        tokenizedNGramList.forEach(dictionary::add);

        ArrayList<Integer> indexedList = dictionary.getIndexListForNGrams(tokenizedNGramList);

        assertTrue(indexedList.size() == tokenizedNGramList.size());

        for(int i = 0 ; i < indexedList.size() ; i++){
            assertEquals(dictionary.getIndex(tokenizedNGramList.get(i)),(int)indexedList.get(i));
        }
    }

}
