package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;
import org.bdp.string_sim.utilities.Tokenizer;


public class IdTokenizedLabelMap extends RichMapFunction<Tuple2<Integer, String>, IdTokenizedLabelTuple2> {

    Tokenizer tokenizer;
    /**
     * Used by the tokenizer. The size of a n-gram.
     */
    private int nGramDigits = 3;

    public IdTokenizedLabelMap(int nGramDigits) {
        if(nGramDigits > 0){
            this.nGramDigits = nGramDigits;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tokenizer = new Tokenizer(this.nGramDigits);
    }

    @Override
    public IdTokenizedLabelTuple2 map(Tuple2<Integer, String> input) throws Exception {
        return new IdTokenizedLabelTuple2(
                input.getField(0),
                tokenizer.tokenize(input.getField(1)).toArray(new String[0])
        );
    }
}
