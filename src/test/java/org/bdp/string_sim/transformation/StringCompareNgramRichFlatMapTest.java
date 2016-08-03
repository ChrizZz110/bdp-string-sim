package org.bdp.string_sim.transformation;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;
import org.bdp.string_sim.types.ResultTuple5;

import junit.framework.TestCase;

public class StringCompareNgramRichFlatMapTest extends TestCase {
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
                new Tuple4<Integer, String, Integer, String>(4, "leipzig", 4, "leipzig"),
                new Tuple4<Integer, String, Integer, String>(3, "dresden", 4, "leipzig")
        );

        DataSet<Tuple2<Integer,String>> idLabelTuple2A = dataSet
                .distinct(0)
                .project(0,1);

        DataSet<Tuple2<Integer,String>> idLabelTuple2 = idLabelTuple2A
                .union(
                        dataSet
                                .distinct(2)
                                .project(2,3)
                )
                .distinct(0);

        DataSet<IdTokenizedLabelTuple2> idTokenizedLabelDataSet = idLabelTuple2
                .map(new IdTokenizedLabelMap(3));

        DataSet<ResultTuple5> algo2ResultDataSet = dataSet
                .flatMap(new StringCompareNgramRichFlatMap())
                .withBroadcastSet(idTokenizedLabelDataSet,"idTokenizedLabelCollection");

        algo2ResultDataSet.print();

        List<ResultTuple5> list = algo2ResultDataSet
                .collect();
        
        assertTrue(list.size() == 5);
        
        for (ResultTuple5 set : list){
        	if ((int)set.getField(0) == 0) assertTrue((float)set.getField(4) == 1.0);
        	if ((int)set.getField(0) == 1) assertTrue((float)set.getField(4) <= 0.7);
        	if ((int)set.getField(0) == 2) assertTrue((float)set.getField(4) == 0.0);
        	if ((int)set.getField(0) == 3) assertTrue((float)set.getField(4) == 0.0);
        	if ((int)set.getField(0) == 4) assertTrue((float)set.getField(4) == 1.0);
        	
        	assertTrue((float)set.getField(4) >= 0.0 && (float)set.getField(4) <= 1.0);
        }

        algo2ResultDataSet = dataSet
                .flatMap(new StringCompareNgramRichFlatMap(0.5))
                .withBroadcastSet(idTokenizedLabelDataSet,"idTokenizedLabelCollection");
        
        list = algo2ResultDataSet
                .collect();
        
        assertTrue(list.size() == 3);
        
        for (ResultTuple5 set : list){
        	if ((int)set.getField(0) == 0) assertTrue((float)set.getField(4) == 1.0);
            if ((int)set.getField(0) == 1) assertTrue((float)set.getField(4) >= 0.518);
        	if ((int)set.getField(0) == 4) assertTrue((float)set.getField(4) == 1.0);
        	
        	assertTrue((float)set.getField(4) >= 0.0 && (float)set.getField(4) <= 1.0);
        }
    }
}
