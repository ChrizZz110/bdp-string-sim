package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.IdTranslatedTokenTuple6;
import org.bdp.string_sim.types.ResultTuple5;

public class FlinkSortMergeFlatMap implements FlatMapFunction<IdTranslatedTokenTuple6,ResultTuple5> {

    /**
     * Only tuples with a similarity value above (>=) the threshold will be collected.
     * The threshold has to be between 0 and 1.
     */
    private double threshold = 0;

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
     * Calculates a similarity value using the sort merge algorithm.
     *
     * @param input see doc of IdTranslatedTokenTuple6
     * @param collector ResultTuple5
     * @throws Exception
     */
    @Override
    public void flatMap(IdTranslatedTokenTuple6 input, Collector<ResultTuple5> collector) throws Exception
    {
        Long[] longArrayA = input.getField(2);
        Long[] longArrayB = input.getField(5);

        int left = 0;
        int right = 0;
        int overlap = 0;
        float diceSim;

        int lengthA = longArrayA.length;
        int lengthB = longArrayB.length;

        while ((left < lengthA) && (right < lengthB))
        {
            if(longArrayA[left].longValue() == longArrayB[right].longValue()){
                overlap++;
                left++;
                right++;
            } else if (longArrayA[left] < longArrayB[right]){
                left++;
            }else {
                right++;
            }
        }

        diceSim = (float) 2 * overlap / (lengthA+lengthB);

        if(diceSim >= threshold){
            collector.collect(new ResultTuple5(
                    input.getField(0),
                    input.getField(1),
                    input.getField(3),
                    input.getField(4),
                    diceSim
            ));
        }
    }
}
