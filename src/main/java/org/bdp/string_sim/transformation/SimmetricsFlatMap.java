package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;
import org.simmetrics.StringMetric;
import org.simmetrics.builders.StringMetricBuilder;
import org.simmetrics.metrics.BlockDistance;
import org.simmetrics.tokenizers.Tokenizers;

public class SimmetricsFlatMap implements FlatMapFunction<Tuple4<Integer, String, Integer, String>, ResultTuple5> {

    /**
     * Only tuples with a similarity value above (>=) the threshold will be collected.
     * The threshold has to be between 0 and 1.
     */
    private double threshold = 0;

    /**
     * Used by the tokenizer. The size of a n-gram.
     */
    private int nGramDigits = 3;

    /**
     * Extended constructor. The threshold and digit size can be specified.
     *
     * @param threshold Only tuples with a similarity value above (>=) the threshold will be collected. The threshold has to be between 0 and 1.
     * @param nGramDigits Used by the tokenizer. The size of a n-gram.
     */
    public SimmetricsFlatMap(double threshold, int nGramDigits) {
        if(threshold >= 0 && threshold <= 1){
            this.threshold = threshold;
        }
        if(nGramDigits > 0){
            this.nGramDigits = nGramDigits;
        }
    }

    /**
     * Calculates string similarity of field 1 and 3 from the input tuple with the simmetrics framework.
     * The following simmetrics parameters are used: BlockDistance algorithmus with qGramWithPadding tokenization
     *
     * @param input the input tuple 4
     * @param collector to collect the output tuple 5
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {

        StringMetric metric = StringMetricBuilder
                .with(new BlockDistance())
                .tokenize(Tokenizers.qGramWithPadding(this.nGramDigits))
                .build();

        float diceSim = metric.compare(input.getField(1), input.getField(3));

        if(diceSim >= this.threshold){
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
