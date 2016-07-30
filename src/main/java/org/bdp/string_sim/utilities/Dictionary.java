package org.bdp.string_sim.utilities;

import org.bdp.string_sim.exception.NotFoundInDictionaryException;

import java.util.ArrayList;
import java.util.TreeMap;

public class Dictionary {

    private final TreeMap<String,Integer> dictionary;
    private int dictionaryPointer = 0;

    public Dictionary(){
        this.dictionary = new TreeMap<>();
    }

    /**
     * Add a String to the dictionary and returns it index
     * @param addString the n-gram to add to dictionary
     * @return the index of this n-gram
     */
    public int add(String addString) {
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
    public void add(ArrayList<String> addList) {
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
