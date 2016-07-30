package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.IdTokenizedLabelTuple4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class FlinkSortMergeFlatMap extends RichFlatMapFunction<IdTokenizedLabelTuple4,Tuple3<Integer,Integer,Float>> {

    /**
     * Only tuples with a similarity value above (>=) the threshold will be collected.
     * The threshold has to be between 0 and 1.
     */
    private double threshold = 0;
    private Collection<Tuple2<Long,String>> dictionary;

    /**
     * Extended constructor. The threshold and digit size can be specified.
     *
     * @param threshold Only tuples with a similarity value above (>=) the threshold will be collected. The threshold has to be between 0 and 1.
     */
    public FlinkSortMergeFlatMap(double threshold) {
        if(threshold >= 0 && threshold <= 1){
            this.threshold = threshold;
        }
    }

    /**
     * Get the dictionary from Broadcast DataSet
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 3. Access the broadcasted DataSet as a Collection
        dictionary = getRuntimeContext().getBroadcastVariable("dictionary");
    }

    /**
     * Calculates a similarity value using the sort merge algorithm. Outputs a Tuple3 :
     * 0 -> ID of LabelA
     * 1 -> ID of LabelB
     * 2 -> Similarity value as Float
     *
     * @param input the IdTokenizedLabelTuple4
     *              value0 the id of label A as int
     *              value1 the tokenized label A as string[]
     *              value2 the id of label B as int
     *              value3 the tokenized label B as string[]
     * @param collector collects the result
     * @throws Exception
     */
    @Override
    public void flatMap(IdTokenizedLabelTuple4 input, Collector<Tuple3<Integer,Integer,Float>> collector) throws Exception
    {
        ArrayList<Long> intArrayA = getIndexListForNGrams(input.getField(1));
        ArrayList<Long> intArrayB = getIndexListForNGrams(input.getField(3));

        Collections.sort(intArrayA);
        Collections.sort(intArrayB);

        int left = 0;
        int right = 0;
        int overlap = 0;
        float diceSim;

        int lengthA = intArrayA.size();
        int lengthB = intArrayB.size();

        while ((left < lengthA) && (right < lengthB))
        {
            if(intArrayA.get(left) == intArrayB.get(right)){
                overlap++;
                left++;
                right++;
            } else if (intArrayA.get(left) < intArrayB.get(right)){
                left++;
            }else {
                right++;
            }
        }

        diceSim = (float) 2 * overlap / (lengthA+lengthB);

        if(diceSim >= threshold){
            collector.collect(new Tuple3<Integer,Integer,Float>(
                    input.getField(0),
                    input.getField(2),
                    diceSim
            ));
        }
    }

    /**
     * Coverts the List of nGrams to a list of corresponding integers using the dictionary DataSet
     * @param nGrams the nGrams to translate
     * @return a translated list of Long values
     */
    private ArrayList<Long> getIndexListForNGrams(String[] nGrams)
    {
        ArrayList<Long> translatedList = new ArrayList<>();

        for (String nGram : nGrams)
        {
            Iterator<Tuple2<Long, String>> iterator = dictionary.iterator();
            while (iterator.hasNext())
            {
                Tuple2<Long, String> dicEntry = iterator.next();
                if(nGram.equals(dicEntry.getField(1)))
                {
                    translatedList.add(dicEntry.getField(0));
                }
            }
        }
        return translatedList;
    }
}
