package org.bdp.string_sim.utilities;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

/**
 * A dictionary that stores Tokens and their corresponding integer values as a flink dataset.
 */
public class FlinkDictionary {
    /**
     * The dictionary DataSet
     */
    private DataSet<Tuple2<Long,String>> dictionary;

    /**
     * A List with all Tokens
     */
    private DataSet<String> nGramStorage;

    /**
     * Used for internal process.
     */
    private boolean calculated = false;

    /**
     * Adds a list of nGrams to the dictionary. Can only done while dictionary was not calculated.
     * @param nGramDataset the DataSet to add
     */
    public void add(DataSet<String> nGramDataset)
    {
        if (!calculated)
        {
            if(nGramStorage == null)
            {
                nGramStorage = nGramDataset;
            } else {
                nGramStorage = nGramStorage.union(nGramDataset);
            }
        } else {
            System.out.println("Can not add to dictionary because it is already calculated.");
        }
    }

    /**
     * Calculates the dictionary. That means distinct the internal list of nGrams, zip it with a unique index.
     */
    private void calculateDictionary()
    {
        nGramStorage = nGramStorage.distinct();
        dictionary = DataSetUtils.zipWithIndex(nGramStorage);
        calculated = true;
    }

    /**
     * Returns the dictionary as flink DataSet with types Tuple2<Long,String>
     * @return the calculated dictionary
     */
    public DataSet<Tuple2<Long,String>> getDictionary()
    {
        if(!calculated)
        {
            this.calculateDictionary();
        }
        return dictionary;
    }
}
