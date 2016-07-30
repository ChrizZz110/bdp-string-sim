package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.IdTokenizedLabelTuple4;

public class CollectTokenFlatMap implements FlatMapFunction<IdTokenizedLabelTuple4,String> {

    /**
     * Collects all tokens from the IdTokenizedLabelTuple4 and puts it in one DataSet of String.
     *
     * @param idTokenizedLabelTuple4 the tokenized id label tuple with tokenized labels at pos 1 and 3
     * @param collector the collector to collect the tokens
     * @throws Exception
     */
    @Override
    public void flatMap(IdTokenizedLabelTuple4 idTokenizedLabelTuple4, Collector<String> collector) throws Exception {
        for (String nGram : (String[]) idTokenizedLabelTuple4.getField(1))
        {
            collector.collect(nGram);
        }
        for (String nGram : (String[]) idTokenizedLabelTuple4.getField(3))
        {
            collector.collect(nGram);
        }
    }
}
