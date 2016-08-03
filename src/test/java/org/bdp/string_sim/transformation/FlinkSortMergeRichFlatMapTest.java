package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.bdp.string_sim.types.IdTokenizedLabelTuple2;
import org.bdp.string_sim.types.ResultTuple5;

import java.util.List;

public class FlinkSortMergeRichFlatMapTest extends TestCase {

    private DataSet<Tuple4<Integer, String, Integer, String>> dataSet;

    public void setUp() throws Exception {
        super.setUp();
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        dataSet = environment.fromElements(
                new Tuple4<Integer, String, Integer, String>(1, "haus",     4, "garten"),
                new Tuple4<Integer, String, Integer, String>(2, "xarten",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(3, "gartex",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(4, "garten",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(5, "gaxten",   4, "garten"),
                new Tuple4<Integer, String, Integer, String>(6, "arten",    4, "garten"),
                new Tuple4<Integer, String, Integer, String>(7, "gar",      4, "garten")
        );
    }

    public void testFlinkSortMergeStd() throws Exception{

        int nGramSize = 3;
        double threshold = 0.0;

        DataSet<Tuple2<Integer,String>> idLabelTuple2A = dataSet
                .distinct(0)
                .project(0,1);

        DataSet<Tuple2<Integer,String>> idLabelTuple2 = idLabelTuple2A
                .union(dataSet.distinct(2)
                        .project(2,3))
                .distinct(0);

        DataSet<IdTokenizedLabelTuple2> idTokenizedLabelDataSet = idLabelTuple2
                .map(new IdTokenizedLabelMap(nGramSize));

        DataSet<String> distinctTokenDataSet = idTokenizedLabelDataSet
                .flatMap(new CollectTokenFromTuple2FlatMap())
                .distinct();

        DataSet<Tuple2<Long, String>> flinkDictionary = DataSetUtils.zipWithUniqueId(distinctTokenDataSet);

        DataSet<Tuple2<Integer,Long[]>> idTranslatedTokenDataSet = idTokenizedLabelDataSet
                .map(new TranslateTokensFromTuple2Map())
                .withBroadcastSet(flinkDictionary,"flinkDictionary");

        DataSet<ResultTuple5> sortMergeResultDataSet = dataSet
                .flatMap(new FlinkSortMergeRichFlatMap(threshold))
                .withBroadcastSet(idTranslatedTokenDataSet,"translatedTokenDictionary");

        List<ResultTuple5> resultTuple5List = sortMergeResultDataSet.collect();

        for (ResultTuple5 resultTuple5 : resultTuple5List)
        {
            switch ((int)resultTuple5.getField(0)){
                case 1:
                    assertEquals((float) 0,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 2:
                    assertEquals((float) 0.625,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 3:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 4:
                    assertEquals((float) 1 ,(float) resultTuple5.getField(4),0.0001);
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

    public void testSortMergeExtended() throws Exception{
        //Set the threshold to 0.65
        int nGramSize = 3;
        double threshold = 0.65;

        DataSet<Tuple2<Integer,String>> idLabelTuple2A = dataSet
                .distinct(0)
                .project(0,1);

        DataSet<Tuple2<Integer,String>> idLabelTuple2 = idLabelTuple2A
                .union(dataSet.distinct(2)
                        .project(2,3))
                .distinct(0);

        DataSet<IdTokenizedLabelTuple2> idTokenizedLabelDataSet = idLabelTuple2
                .map(new IdTokenizedLabelMap(nGramSize));

        DataSet<String> distinctTokenDataSet = idTokenizedLabelDataSet
                .flatMap(new CollectTokenFromTuple2FlatMap())
                .distinct();

        DataSet<Tuple2<Long, String>> flinkDictionary = DataSetUtils.zipWithUniqueId(distinctTokenDataSet);

        DataSet<Tuple2<Integer,Long[]>> idTranslatedTokenDataSet = idTokenizedLabelDataSet
                .map(new TranslateTokensFromTuple2Map())
                .withBroadcastSet(flinkDictionary,"flinkDictionary");

        DataSet<ResultTuple5> sortMergeResultDataSet = dataSet
                .flatMap(new FlinkSortMergeRichFlatMap(threshold))
                .withBroadcastSet(idTranslatedTokenDataSet,"translatedTokenDictionary");

        sortMergeResultDataSet.print();

        List<ResultTuple5> resultTuple5List = sortMergeResultDataSet.collect();

        // Only 2 of the 5 tuples have a similarity value above 0.5
        assertEquals(2,resultTuple5List.size());

        for (ResultTuple5 resultTuple5 : resultTuple5List)
        {
            switch ((int)resultTuple5.getField(0)){
                case 4:
                    assertEquals((float) 1 ,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 6:
                    assertEquals((float) (2.0/3) ,(float) resultTuple5.getField(4),0.0001);
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        }
    }

    public void testSortMergeExtendedBiGram() throws Exception{
        //Set the threshold to 0.65 and n-gram digit size to 2
        int nGramSize = 2;
        double threshold = 0.65;

        DataSet<Tuple2<Integer,String>> idLabelTuple2A = dataSet
                .distinct(0)
                .project(0,1);

        DataSet<Tuple2<Integer,String>> idLabelTuple2 = idLabelTuple2A
                .union(dataSet.distinct(2)
                        .project(2,3))
                .distinct(0);

        DataSet<IdTokenizedLabelTuple2> idTokenizedLabelDataSet = idLabelTuple2
                .map(new IdTokenizedLabelMap(nGramSize));

        DataSet<String> distinctTokenDataSet = idTokenizedLabelDataSet
                .flatMap(new CollectTokenFromTuple2FlatMap())
                .distinct();

        DataSet<Tuple2<Long, String>> flinkDictionary = DataSetUtils.zipWithUniqueId(distinctTokenDataSet);

        DataSet<Tuple2<Integer,Long[]>> idTranslatedTokenDataSet = idTokenizedLabelDataSet
                .map(new TranslateTokensFromTuple2Map())
                .withBroadcastSet(flinkDictionary,"flinkDictionary");

        DataSet<ResultTuple5> sortMergeResultDataSet = dataSet
                .flatMap(new FlinkSortMergeRichFlatMap(threshold))
                .withBroadcastSet(idTranslatedTokenDataSet,"translatedTokenDictionary");

        sortMergeResultDataSet.print();

        List<ResultTuple5> resultTuple5List = sortMergeResultDataSet.collect();

        // Only 2 of the 5 tuples have a similarity value above 0.5
        assertEquals(2,resultTuple5List.size());

        for (ResultTuple5 resultTuple5 : resultTuple5List)
        {
            switch ((int)resultTuple5.getField(0)){
                case 4:
                    assertEquals((float) 1 ,(float) resultTuple5.getField(4),0.0001);
                    break;
                case 6:
                    assertEquals((float) 2/3.0 ,(float) resultTuple5.getField(4),0.0001);
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        }
    }
}
