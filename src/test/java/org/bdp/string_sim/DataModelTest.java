package org.bdp.string_sim;

import junit.framework.TestCase;

import java.util.ArrayList;

public class DataModelTest extends TestCase {

    private ArrayList<String> tokenizedNGramList = new ArrayList<>();

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
        DataModel dataModel = new DataModel();

        tokenizedNGramList.forEach(dataModel::addToDictionary);

        for (String trigram : tokenizedNGramList) {
            assertTrue(dataModel.getDictionary().containsKey(trigram));
        }

        assertEquals(dataModel.getIndex(tokenizedNGramList.get(3)),dataModel.getIndex(tokenizedNGramList.get(7)));
        assertEquals(dataModel.getIndex(tokenizedNGramList.get(4)),dataModel.getIndex(tokenizedNGramList.get(8)));

    }

    public void testAddListToDictionary() throws Exception{
        DataModel dataModel = new DataModel();

        dataModel.addToDictionary(tokenizedNGramList);

        for (String trigram : tokenizedNGramList) {
            assertTrue(dataModel.getDictionary().containsKey(trigram));
        }

        assertEquals(dataModel.getIndex(tokenizedNGramList.get(3)),dataModel.getIndex(tokenizedNGramList.get(7)));
        assertEquals(dataModel.getIndex(tokenizedNGramList.get(4)),dataModel.getIndex(tokenizedNGramList.get(8)));

    }

    public void testGetIndexListForNGrams() throws Exception{
        DataModel dataModel = new DataModel();

        tokenizedNGramList.forEach(dataModel::addToDictionary);

        ArrayList<Integer> indexedList = dataModel.getIndexListForNGrams(tokenizedNGramList);

        assertTrue(indexedList.size() == tokenizedNGramList.size());

        for(int i = 0 ; i < indexedList.size() ; i++){
            assertEquals(dataModel.getIndex(tokenizedNGramList.get(i)),(int)indexedList.get(i));
        }
    }
}
