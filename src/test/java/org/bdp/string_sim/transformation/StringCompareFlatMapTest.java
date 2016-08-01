package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.ResultTuple5;

import java.util.List;

public class StringCompareFlatMapTest extends TestCase {
    private ExecutionEnvironment environment;

    public void setUp() throws Exception {
        super.setUp();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void testStringCompareMap() throws Exception{
        DataSet<Tuple4<Integer, String, Integer, String>> dataSet = environment.fromElements(
                new Tuple4<Integer, String, Integer, String>(1, "haus", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(2, "garten", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(3, "Garten", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(4, "garten ", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(5, "_garten", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(6, "garteN", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(7, "garteN", 4, "garteN")
        );

        List<ResultTuple5> list = dataSet.flatMap(new StringCompareFlatMap()).collect();

        assertTrue(list.size() == 7);

        for(ResultTuple5 rt : list){
            if((int)rt.getField(0) == 2 || (int)rt.getField(0) == 7)
            {
                assertTrue((float) rt.getField(4) == 1.0);
            }else {
                assertTrue((float) rt.getField(4) == 0.0);
            }
        }

        list = dataSet.flatMap(new StringCompareFlatMap(true)).collect();

        assertTrue(list.size() == 2);

        for(ResultTuple5 rt : list){
            assertTrue((float) rt.getField(4) == 1.0);
        }
    }
}
