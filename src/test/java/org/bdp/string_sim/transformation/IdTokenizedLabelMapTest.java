package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;

import java.util.List;

public class IdTokenizedLabelMapTest extends TestCase {
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

    public void testIdTokenizeLabelMap () throws Exception
    {
        int tokenSize = 4;

        DataSet<Tuple2<Integer,String>> integerStringDataSet = dataSet.project(0,1);

        List<IdTokenizedLabelTuple2> list = integerStringDataSet.map(new IdTokenizedLabelMap(tokenSize)).collect();

        assertEquals(7,list.size());

        for (IdTokenizedLabelTuple2 tuple2 : list)
        {
            switch ((int) tuple2.getField(0)) {
                case 1:
                    String[] tokens = tuple2.getField(1);
                    assertEquals(7,tokens.length);
                    assertEquals("###h",tokens[0]);
                    assertEquals("s###",tokens[6]);
                    break;
            }
        }
    }
}
