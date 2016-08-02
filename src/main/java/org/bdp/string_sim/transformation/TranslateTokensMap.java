package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.bdp.string_sim.types.IdTokenizedLabelTuple6;
import org.bdp.string_sim.types.IdTranslatedTokenTuple6;
import org.bdp.string_sim.utilities.Dictionary;

import java.util.Collection;

public class TranslateTokensMap extends RichMapFunction<IdTokenizedLabelTuple6,IdTranslatedTokenTuple6> {

    private Dictionary dictionary;

    @Override
    public void open(Configuration parameters) throws Exception {
        Collection<Tuple2<Long,String>> flinkDictionary = getRuntimeContext().getBroadcastVariable("flinkDictionary");
        dictionary = new Dictionary();
        dictionary.add(flinkDictionary);
    }

    /**
     * Exchanges the array of tokens with its corresponding array of long values using the dictionary
     * From <Integer,String,String[],Integer,String,String[]> to <Integer,String,Long[],Integer,String,Long[]>
     * @param input IdTokenizedLabelTuple6
     * @return a IdTranslatedTokenTuple6
     * @throws Exception
     */
    @Override
    public IdTranslatedTokenTuple6 map(IdTokenizedLabelTuple6 input) throws Exception {
        return new IdTranslatedTokenTuple6(
                input.getField(0),
                input.getField(1),
                dictionary.getSortedIndexArrayForNGrams(input.getField(2)),
                input.getField(3),
                input.getField(4),
                dictionary.getSortedIndexArrayForNGrams(input.getField(5))
        );
    }
}
