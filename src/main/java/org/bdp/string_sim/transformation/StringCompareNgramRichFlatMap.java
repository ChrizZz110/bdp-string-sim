package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;
import org.bdp.string_sim.types.ResultTuple5;
import org.bdp.string_sim.utilities.DiceMetric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;

public class StringCompareNgramRichFlatMap extends RichFlatMapFunction<Tuple4<Integer, String, Integer, String>,ResultTuple5>{

    private TreeMap<Integer,String[]> treeMap;
    private double threshold = 0.0;
    private Collection<IdTokenizedLabelTuple2> idTokenizedLabelCollection;

    public StringCompareNgramRichFlatMap() {
    }

    public StringCompareNgramRichFlatMap(double threshold) {
        if(threshold >= 0 && threshold <= 1){
            this.threshold = threshold;
        }
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        idTokenizedLabelCollection = getRuntimeContext().getBroadcastVariable("idTokenizedLabelCollection");
        treeMap = new TreeMap<Integer, String[]>();
        // Fill the TreeMap once
        for (IdTokenizedLabelTuple2 idTranslatedToken : idTokenizedLabelCollection)
        {
            treeMap.put(idTranslatedToken.getField(0),idTranslatedToken.getField(1));
        }
    }

    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {

        ArrayList<String> stringListA = new ArrayList<>(Arrays.asList(treeMap.get((Integer) input.getField(0))));
        ArrayList<String> stringListB = new ArrayList<>(Arrays.asList(treeMap.get((Integer) input.getField(2))));

        Integer sizeListA = stringListA.size();
        Integer sizeListB = stringListB.size();
        Integer match = 0;

        for (String a : stringListA) {
            for (String b : stringListB) {
                if (a.equals(b)) {
                    match++;
                    // only one match per token due to integrity of diceMetric
                    stringListB.remove(b);
                    break;
                }
            }
        }

        float diceMetric = DiceMetric.calculate(sizeListA, sizeListB, match);

        if (diceMetric >= this.threshold) {
            collector.collect(
                    new ResultTuple5(
                            input.getField(0),
                            input.getField(1),
                            input.getField(2),
                            input.getField(3),
                            diceMetric
                    )
            );
        }
    }
}
