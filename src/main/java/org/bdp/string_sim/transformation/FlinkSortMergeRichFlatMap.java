package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;

import java.util.Collection;
import java.util.TreeMap;

public class FlinkSortMergeRichFlatMap extends RichFlatMapFunction<Tuple4<Integer, String, Integer, String>, ResultTuple5>{

    private TreeMap<Integer,Long[]> translatedTokenTreeMap1;
    private double threshold = 0;
    private Collection<Tuple2<Integer,Long[]>> tuple2Collection;

    public FlinkSortMergeRichFlatMap(double threshold) {
        if(threshold >= 0 && threshold <= 1){
            this.threshold = threshold;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tuple2Collection = getRuntimeContext().getBroadcastVariable("translatedTokenDictionary");
        translatedTokenTreeMap1 = new TreeMap<Integer, Long[]>();
        // Fill the TreeMap once
        for (Tuple2<Integer,Long[]> idTranslatedToken : tuple2Collection)
        {
            translatedTokenTreeMap1.put(idTranslatedToken.getField(0),idTranslatedToken.getField(1));
        }
    }

    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {

        Long[] longArrayA = translatedTokenTreeMap1.get(input.getField(0));
        Long[] longArrayB = translatedTokenTreeMap1.get(input.getField(2));

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
                    input.getField(2),
                    input.getField(3),
                    diceSim
            ));
        }
    }
}
