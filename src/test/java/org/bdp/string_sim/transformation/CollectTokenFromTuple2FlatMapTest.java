package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.IdLabelCompareTuple4;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;

public class CollectTokenFromTuple2FlatMapTest extends TestCase {
    private ExecutionEnvironment environment;

    public void setUp() throws Exception {
        super.setUp();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void testCollectTokenFromTuple2FlatMapTest() throws Exception{
        DataSet<Tuple4<Integer,String,Integer,String>> dataSet = environment.fromElements(
                new IdLabelCompareTuple4(1, "haus", 4, "garten"),
                new IdLabelCompareTuple4(2, "garen", 4, "garten"),
                new IdLabelCompareTuple4(3, "gartenstuhl", 4, "garten"),
                new IdLabelCompareTuple4(4, "garten", 4, "garten"),
                new IdLabelCompareTuple4(5, "_garten", 4, "garten"),
                new IdLabelCompareTuple4(6, "gargartenten", 4, "garten"),
                new IdLabelCompareTuple4(7, "gartem", 4, "garten")
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

        DataSet<String> nGramCollection = idTokenizedLabelDataSet
                .flatMap(new CollectTokenFromTuple2FlatMap());

        assertEquals(65,nGramCollection
                .count()
        );

        assertEquals(33,nGramCollection
                .distinct()
                .count()
        );
    }
}
