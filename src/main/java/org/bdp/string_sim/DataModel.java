package org.bdp.string_sim;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.exception.NotFoundInDictionaryException;

import java.util.ArrayList;
import java.util.TreeMap;

public class DataModel {
    public static final int CONCEPT_COL_ENTITY_ID = 0;
    public static final int CONCEPT_COL_URI = 1;
    public static final int CONCEPT_COL_SRC = 2;

    public static final int CONCEPT_ATTR_COL_ENTITY_ID = 0;
    public static final int CONCEPT_ATTR_COL_PROP_NAME = 1;
    public static final int CONCEPT_ATTR_COL_PROP_VAL = 2;
    public static final int CONCEPT_ATTR_COL_PROP_TYPE = 3;

    public static final int LINKS_COL_SRC_ID = 0;
    public static final int LINKS_COL_TRG_ID = 1;

    private DataSet<Tuple3<Integer,String,String>> conceptDataSet;
    private DataSet<Tuple4<Integer,String,String,String>> conceptAttrDataSet;
    private DataSet<Tuple2<Integer,Integer>> linksWithIDsDataSet;

    private TreeMap<String,Integer> dictionary;
    private int dictionaryPointer = 0;

    public DataModel() {
        this.dictionary = new TreeMap<>();
    }

    public DataSet<Tuple3<Integer, String, String>> getConceptDataSet() {
        return conceptDataSet;
    }

    public void setConceptDataSet(DataSet<Tuple3<Integer, String, String>> conceptDataSet) {
        this.conceptDataSet = conceptDataSet;
    }

    public DataSet<Tuple4<Integer, String, String, String>> getConceptAttrDataSet() {
        return conceptAttrDataSet;
    }

    public void setConceptAttrDataSet(DataSet<Tuple4<Integer, String, String, String>> conceptAttrDataSet) {
        this.conceptAttrDataSet = conceptAttrDataSet;
    }

    public DataSet<Tuple2<Integer, Integer>> getLinksWithIDsDataSet() {
        return linksWithIDsDataSet;
    }

    public void setLinksWithIDsDataSet(DataSet<Tuple2<Integer, Integer>> linksWithIDsDataSet) {
        this.linksWithIDsDataSet = linksWithIDsDataSet;
    }

    /**
     * Add a String to the dictionary and returns it index
     * @param addString the n-gram to add to dictionary
     * @return the index of this n-gram
     */
    public int addToDictionary(String addString) {
        if(dictionary.containsKey(addString))
        {
            return dictionary.get(addString);
        }else {
            this.dictionary.put(addString,dictionaryPointer);
            return dictionaryPointer++;
        }
    }

    /**
     * Add a list of n-grams to the dictionary.
     *
     * @param addList the list of n-gram to add to dictionary
     */
    public void addToDictionary(ArrayList<String> addList) {
        for(String nGram : addList){
            if(!dictionary.containsKey(nGram))
            {
                this.dictionary.put(nGram,dictionaryPointer++);
            }
        }
    }

    /**
     * Gett for dictionary
     *
     * @return the dictionary: key = n-gram; value = index
     */
    public TreeMap<String, Integer> getDictionary() {
        return dictionary;
    }

    /**
     * Get the index of the given nGram.
     *
     * @param nGram the nGram to search for
     * @return the index as int
     * @throws NotFoundInDictionaryException if the n-gram was never added to th dictionary
     */
    public int getIndex(String nGram) throws NotFoundInDictionaryException{
        if(dictionary.containsKey(nGram)) {
            return dictionary.get(nGram);
        }else {
            throw new NotFoundInDictionaryException(nGram);
        }
    }

    /**
     * Maps a List of n-grams to the corresponding list of indexes.
     *
     * @param tokenizedList a List of n-grams
     * @return the corresponding list of indexes
     * @throws NotFoundInDictionaryException if one of the n-gram was never added to th dictionary
     */
    public ArrayList<Integer> getIndexListForNGrams(ArrayList<String> tokenizedList) throws NotFoundInDictionaryException{
        ArrayList<Integer> indexedList = new ArrayList<>();
        for(String nGram : tokenizedList){
            indexedList.add(getIndex(nGram));
        }
        return indexedList;
    }
}
