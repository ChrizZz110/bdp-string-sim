package org.bdp.string_sim.transformation;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.ResultTuple5;

import junit.framework.TestCase;

public class StringCompareTrigramFlatMapTest extends TestCase {
    private ExecutionEnvironment environment;

    public void setUp() throws Exception {
        super.setUp();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }
    
    public void testStringCompareTrigram() throws Exception{
    	DataSet<Tuple4<Integer, String, Integer, String>> dataSet = environment.fromElements(
                new Tuple4<Integer, String, Integer, String>(0, "leipzig", 4, "leipzig"),
                new Tuple4<Integer, String, Integer, String>(1, "leipzig(sachsen)", 4, "leipzig"),
                new Tuple4<Integer, String, Integer, String>(2, "LEIPZIG", 4, "leipzig"),
                new Tuple4<Integer, String, Integer, String>(3, "leipzig ", 4, "leipzig"),
                new Tuple4<Integer, String, Integer, String>(4, "dresden", 4, "leipzig")
        );

        List<ResultTuple5> list = dataSet.flatMap(new StringCompareTrigramFlatMap((float)0.0,3)).collect();
        
        assertTrue(list.size() == 5);
        
        for (ResultTuple5 set : list){
        	if ((int)set.getField(0) == 0) assertTrue((float)set.getField(4) == 1.0);
        	if ((int)set.getField(0) == 1) assertTrue((float)set.getField(4) <= 0.7);
        	if ((int)set.getField(0) == 2) assertTrue((float)set.getField(4) == 0.0);
        	if ((int)set.getField(0) == 3) assertTrue((float)set.getField(4) >= 0.7);
        	if ((int)set.getField(0) == 4) assertTrue((float)set.getField(4) == 0.0);
        	
        	assertTrue((float)set.getField(4) >= 0.0 && (float)set.getField(4) <= 1.0);
        }
        
        list = dataSet.flatMap(new StringCompareTrigramFlatMap()).collect();
        
        assertTrue(list.size() == 2);
        
        for (ResultTuple5 set : list){
        	if ((int)set.getField(0) == 0) assertTrue((float)set.getField(4) == 1.0);
        	if ((int)set.getField(0) == 3) assertTrue((float)set.getField(4) >= 0.7);
        	
        	assertTrue((float)set.getField(4) >= 0.0 && (float)set.getField(4) <= 1.0);
        }
    }
}
