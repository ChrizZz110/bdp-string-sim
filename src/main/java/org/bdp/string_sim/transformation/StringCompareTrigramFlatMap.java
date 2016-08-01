package org.bdp.string_sim.transformation;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;
import org.bdp.string_sim.utilities.DiceMetric;
import org.bdp.string_sim.utilities.Tokenizer;


public class StringCompareTrigramFlatMap implements FlatMapFunction<Tuple4<Integer,String,Integer,String>,ResultTuple5> {

    private float thresholdMatch = (float)0.7;
	private int nGramSize = 3;

    public StringCompareTrigramFlatMap() {
    }
    
    /**
     * This constructor creates an StringCompareTrigramFlatMap instance.
     * @param thresholdMatch only collects matches equal or above this threshold. threshold must be between 0.0 and 1.0.
     */
    public StringCompareTrigramFlatMap(float thresholdMatch, int nGramSize) {
    	if (thresholdMatch >= 0.0 && thresholdMatch <= 1.0) {
    		this.thresholdMatch = thresholdMatch;
    	}
		if (nGramSize > 0 && nGramSize < 10)
		{
			this.nGramSize = nGramSize;
		}
    }
    
    /**
     * This flatMap function compares strings in field 1 and 3 of the input tuple4 by tokenization its strings and compare it against
     * each other. According to matching tokens a diceMetric is calculated and added in a ResultTuple5. Matches will be only be saved if they 
     * have a sufficient diceMetric, according to propterty thresholdMatch.
     * @param input the input tuple4:
     *              Field 0: the id of label A
     *              Field 1: the label A
     *              Field 2: the id of label B
     *              Field 3: the label B
     * @param collector the collector which collects all result tuple of type ResultTuple5
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {
    	
    	Tokenizer tokenizer = new Tokenizer(nGramSize);
    	ArrayList <String> stringListA = tokenizer.tokenize(input.getField(1));
    	ArrayList <String> stringListB = tokenizer.tokenize(input.getField(3));
    	Integer sizeListA = stringListA.size();
    	Integer sizeListB = stringListB.size();
    	Integer match = 0;
    	
    	for (String a : stringListA){
    		for (String b : stringListB){
    			if (a.equals(b)){
    				match++;
    				// only one match per token due to integrity of diceMetric
    				stringListB.remove(b);
    				break;
    			}
    		}
    	}
    	
    	float diceMetric = DiceMetric.calculate(sizeListA, sizeListB, match);
    	
    	if (diceMetric >= thresholdMatch) 
    		collector.collect(new ResultTuple5(
								    			input.getField(0),
								    			input.getField(1),
								    			input.getField(2),
								    			input.getField(3),
								    			diceMetric
								    			));
    }
}