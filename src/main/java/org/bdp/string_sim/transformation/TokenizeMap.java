package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.IdTokenizedLabelTuple4;
import org.bdp.string_sim.utilities.Tokenizer;

import java.util.ArrayList;

public class TokenizeMap extends RichMapFunction<Tuple4<Integer,String,Integer,String>, IdTokenizedLabelTuple4> {

    /**
     * Used by the tokenizer. The size of a n-gram.
     */
    private int nGramDigits = 3;


    public TokenizeMap(int nGramDigits) {
        if(nGramDigits > 0){
            this.nGramDigits = nGramDigits;
        }
    }

    /**
     * Tokenizes the labels inside the tuple 4: string -> string[]
     * @param idLabelCompareTuple4 the input entity
     * @return IdTokenizedLabelTuple4 with the tokenized labels
     * @throws Exception
     */
    @Override
    public IdTokenizedLabelTuple4 map(Tuple4<Integer,String,Integer,String> idLabelCompareTuple4) throws Exception {
        Tokenizer tokenizer = new Tokenizer(nGramDigits);

        ArrayList<String> tokenizedLabelA = tokenizer.tokenize(idLabelCompareTuple4.getField(1));
        ArrayList<String> tokenizedLabelB = tokenizer.tokenize(idLabelCompareTuple4.getField(3));

        String[] tokensA = new String[tokenizedLabelA.size()];
        tokensA = tokenizedLabelA.toArray(tokensA);
        String[] tokensB = new String[tokenizedLabelB.size()];
        tokensB = tokenizedLabelB.toArray(tokensB);

        return new IdTokenizedLabelTuple4(idLabelCompareTuple4.getField(0),tokensA,idLabelCompareTuple4.getField(2),tokensB);
    }
}
