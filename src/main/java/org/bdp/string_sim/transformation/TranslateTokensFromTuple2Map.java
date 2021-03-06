package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;
import org.bdp.string_sim.utilities.Dictionary;

import java.util.Collection;

public class TranslateTokensFromTuple2Map extends RichMapFunction<IdTokenizedLabelTuple2, Tuple2<Integer, Long[]>> {
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
    public Tuple2<Integer, Long[]> map(IdTokenizedLabelTuple2 input) throws Exception {
        return new Tuple2<Integer, Long[]>(
                input.getField(0),
                dictionary.getSortedIndexArrayForNGrams(input.getField(1))
        );
    }
}
