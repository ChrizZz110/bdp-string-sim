package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.ResultTuple5;
import java.util.List;

public class SimmetricsFlatMapTest extends TestCase{
    private DataSet<Tuple4<Integer, String, Integer, String>> dataSet;

    public void setUp() throws Exception {
        super.setUp();
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        dataSet = environment.fromElements(
                new Tuple4<Integer, String, Integer, String>(1, "haus", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(2, "garten", 4, "haus"),
                new Tuple4<Integer, String, Integer, String>(3, "gartex", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(4, "xarten", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(5, "gaxten", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(6, "arten", 4, "garten"),
                new Tuple4<Integer, String, Integer, String>(7, "gar", 4, "garten")
        );
    }

    public void testSimmetricsStd() throws Exception{
        double threshold = 0.0;
        int nGramSize = 3;
        List<ResultTuple5> list = dataSet.flatMap(new SimmetricsFlatMap(threshold,nGramSize)).collect();

        assertEquals(7,list.size());

        for (ResultTuple5 resultTuple5 : list)
        {
            switch ((int)resultTuple5.getField(0)){
                case 1:
                    assertEquals((float) 0,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 2:
                    assertEquals((float) 0,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 3:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 4:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 5:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 6:
                    assertEquals((float) (2.0/3) ,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 7:
                    assertEquals((float) (6.0/13) ,(float) resultTuple5.getField(4),0.0001);
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        }
    }
}
