package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class StrictUpperTriangularMatrixFilterTest extends TestCase{
    private ExecutionEnvironment environment;

    public void setUp() throws Exception {
        super.setUp();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * Tests the Label Filter class
     *
     * @throws Exception
     */
    public void testFilter() throws Exception{
        DataSet<Tuple2<Integer,Integer>> dataSet = environment.fromElements(
                new Tuple2<Integer, Integer>(1,1),
                new Tuple2<Integer, Integer>(1,2),
                new Tuple2<Integer, Integer>(1,3),
                new Tuple2<Integer, Integer>(2,1),
                new Tuple2<Integer, Integer>(2,2),
                new Tuple2<Integer, Integer>(2,3),
                new Tuple2<Integer, Integer>(3,1),
                new Tuple2<Integer, Integer>(3,2),
                new Tuple2<Integer, Integer>(3,3)
        );

        dataSet = dataSet.filter(new StrictUpperTriangularMatrixFilter());
        assertEquals(3,dataSet.count());

        List<Tuple2<Integer, Integer>> tuple2List = dataSet.collect();

        for(Tuple2<Integer, Integer> tuple2 : tuple2List){
            assertTrue( (int) tuple2.getField(0) > (int) tuple2.getField(1) );
        }
    }
}
