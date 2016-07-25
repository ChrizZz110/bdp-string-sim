package org.bdp.string_sim.transformation;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.ResultTuple5;
import org.bdp.string_sim.utilities.DiceMetric;
import org.bdp.string_sim.utilities.Tokenizer;


public class StringCompareTrigram implements FlatMapFunction<Tuple4<Integer,String,Integer,String>,ResultTuple5> {

    private float thresholdMatch = (float)0.7;

    public StringCompareTrigram() {
    }

    public StringCompareTrigram(float collectOnlyMatchingStrings) {
        this.thresholdMatch = collectOnlyMatchingStrings;
    }

    @Override
    public void flatMap(Tuple4<Integer, String, Integer, String> input, Collector<ResultTuple5> collector) throws Exception {
    	
    	Tokenizer tokenizer = new Tokenizer();
    	ArrayList <String> stringListA = tokenizer.tokenize(input.getField(1));
    	ArrayList <String> stringListB = tokenizer.tokenize(input.getField(3));
    	
    	Integer match = 0;
    	for (String a : stringListA){
    		for (String b : stringListB){
    			if (a.equals(b)){
    				match++;
    				break;
    			}
    		}
    	}
    	
    	float diceMetric = DiceMetric.calculate(stringListA.size(), stringListB.size(), match);
    	
    	//if (diceMetric >= thresholdMatch) 
    		collector.collect(new ResultTuple5(
								    			input.getField(0),
								    			input.getField(1),
								    			input.getField(2),
								    			input.getField(3),
								    			diceMetric
								    			));
    }
}