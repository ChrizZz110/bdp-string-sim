package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.IdTokenizedLabelTuple6;

public class CollectTokenFlatMap implements FlatMapFunction<IdTokenizedLabelTuple6,String> {

    /**
     * Collects all tokens from the IdTokenizedLabelTuple6 and puts it in one DataSet of String.
     *
     * @param idTokenizedLabelTuple4 the tokenized id label tuple with tokenized labels at pos 1 and 3
     * @param collector the collector to collect the tokens
     * @throws Exception
     */
    @Override
    public void flatMap(IdTokenizedLabelTuple6 idTokenizedLabelTuple4, Collector<String> collector) throws Exception {
        for (String nGram : (String[]) idTokenizedLabelTuple4.getField(2))
        {
            collector.collect(nGram);
        }
        for (String nGram : (String[]) idTokenizedLabelTuple4.getField(5))
        {
            collector.collect(nGram);
        }
    }
}
