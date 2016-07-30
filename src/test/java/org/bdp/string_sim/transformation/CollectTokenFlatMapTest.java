package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.IdLabelCompareTuple4;

public class CollectTokenFlatMapTest extends TestCase {
    private ExecutionEnvironment environment;

    public void setUp() throws Exception {
        super.setUp();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void testCollectTokenFlatMapTest() throws Exception{
        DataSet<Tuple4<Integer,String,Integer,String>> dataSet = environment.fromElements(
                new IdLabelCompareTuple4(1, "haus", 4, "garten"),
                new IdLabelCompareTuple4(2, "garen", 4, "garten"),
                new IdLabelCompareTuple4(3, "gartenstuhl", 4, "garten"),
                new IdLabelCompareTuple4(4, "garten", 4, "garten"),
                new IdLabelCompareTuple4(5, "_garten", 4, "garten"),
                new IdLabelCompareTuple4(6, "gargartenten", 4, "garten"),
                new IdLabelCompareTuple4(7, "gartem", 4, "garten")
        );

        DataSet<String> nGramCollection = dataSet
                .map(new TokenizeMap(3))
                .flatMap(new CollectTokenFlatMap());

        assertEquals(121,nGramCollection
                .count()
        );

        assertEquals(33,nGramCollection
                .distinct()
                .count()
        );
    }
}
