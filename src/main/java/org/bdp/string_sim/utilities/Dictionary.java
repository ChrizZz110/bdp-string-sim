package org.bdp.string_sim.utilities;

import org.apache.flink.api.java.tuple.Tuple2;
import org.bdp.string_sim.exception.NotFoundInDictionaryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;

public class Dictionary {

    private final TreeMap<String,Long> dictionary;
    private long dictionaryPointer = 0;

    public Dictionary(){
        this.dictionary = new TreeMap<>();
    }

    /**
     * Add a String to the dictionary and returns it index
     * @param addString the n-gram to add to dictionary
     * @return the index of this n-gram
     */
    public long add(String addString) {
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

    public void add(Collection<Tuple2<Long,String>> flinkDictionary)
    {
        for(Tuple2<Long,String> flinkDicEntry : flinkDictionary){
            if(!dictionary.containsKey(flinkDicEntry.getField(1)))
            {
                this.dictionary.put(flinkDicEntry.getField(1),flinkDicEntry.getField(0));
            }
        }
    }

    /**
     * Gett for dictionary
     *
     * @return the dictionary: key = n-gram; value = index
     */
    public TreeMap<String, Long> getDictionary() {
        return dictionary;
    }

    /**
     * Get the index of the given nGram.
     *
     * @param nGram the nGram to search for
     * @return the index as int
     * @throws NotFoundInDictionaryException if the n-gram was never added to th dictionary
     */
    public long getIndex(String nGram) throws NotFoundInDictionaryException{
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
    public ArrayList<Long> getIndexListForNGrams(ArrayList<String> tokenizedList) throws NotFoundInDictionaryException{
        ArrayList<Long> indexedList = new ArrayList<>();
        for(String nGram : tokenizedList){
            indexedList.add(getIndex(nGram));
        }
        return indexedList;
    }

    public Long[] getSortedIndexArrayForNGrams(String[] tokenizedList) throws NotFoundInDictionaryException{
        Long[] indexedList = new Long[tokenizedList.length];
        for(int i = 0 ; i < tokenizedList.length ; i++){
            indexedList[i] = getIndex(tokenizedList[i]);
        }
        Arrays.sort(indexedList);
        return indexedList;
    }
}
