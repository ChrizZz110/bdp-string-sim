package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;

import java.util.List;

public class TranslateTokensFromTuple2MapTest extends TestCase {
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

    public void testTranslateTokensFromTuple2Map () throws Exception
    {
        int tokenSize = 4;

        DataSet<Tuple2<Integer,String>> integerStringDataSet = dataSet.project(0,1);

        DataSet<IdTokenizedLabelTuple2> idTokenizedLabelTuple2DataSet = integerStringDataSet.map(new IdTokenizedLabelMap(tokenSize));

        DataSet<String> distinctTokenDataSet = idTokenizedLabelTuple2DataSet
                .flatMap(new CollectTokenFromTuple2FlatMap())
                .distinct();

        DataSet<Tuple2<Long, String>> flinkDictionary = DataSetUtils.zipWithUniqueId(distinctTokenDataSet);

        List<Tuple2<Integer,Long[]>> list = idTokenizedLabelTuple2DataSet
                .map(new TranslateTokensFromTuple2Map())
                .withBroadcastSet(flinkDictionary,"flinkDictionary")
                .collect();

        assertEquals(7,list.size());

        for (Tuple2<Integer,Long[]> tuple2 : list)
        {
            switch ((int) tuple2.getField(0)) {
                case 1:
                    Long[] transTokens = tuple2.getField(1);
                    assertEquals(7,transTokens.length);
                    break;
                case 2:
                    Long[] transTokens2 = tuple2.getField(1);
                    assertEquals(9,transTokens2.length);
                    break;
            }
        }
    }
}
