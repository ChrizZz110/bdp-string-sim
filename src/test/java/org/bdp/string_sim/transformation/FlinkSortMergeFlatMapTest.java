package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.types.IdTokenizedLabelTuple4;
import org.bdp.string_sim.utilities.FlinkDictionary;

import java.util.List;

public class FlinkSortMergeFlatMapTest extends TestCase {

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

    public void testSortMergeStd() throws Exception{

        int nGramSize = 3;
        double threshold = 0.0;

        FlinkDictionary dictionary = new FlinkDictionary();

        DataSet<IdTokenizedLabelTuple4> tokenizedLabelDataset = dataSet
                .map(new TokenizeMap(nGramSize));

        DataSet<String> nGramCollection = tokenizedLabelDataset
                .flatMap(new CollectTokenFlatMap());

        nGramCollection = nGramCollection.distinct();

        dictionary.add(nGramCollection);

        DataSet<Tuple2<Long, String>> dictionaryDataSet = dictionary.getDictionary();

        DataSet<Tuple3<Integer,Integer,Float>> sortMergeResultDataSet = tokenizedLabelDataset
                .flatMap(new FlinkSortMergeFlatMap(threshold))
                .withBroadcastSet(dictionaryDataSet,"dictionary");

        List<Tuple3<Integer,Integer,Float>> list = sortMergeResultDataSet.collect();

        System.out.println(list);

        for (Tuple3<Integer,Integer,Float> resultTuple5 : list)
        {
            switch ((int)resultTuple5.getField(0)){
                case 1:
                    assertEquals((float) 0,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 2:
                    assertEquals((float) 0,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 3:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 4:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 5:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 6:
                    assertEquals((float) (2.0/3) ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 7:
                    assertEquals((float) (6.0/13) ,(float) resultTuple5.getField(2),0.0001);
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        }
    }

    public void testSortMergeExtended() throws Exception{
        //Set the threshold to 0.5
        int nGramSize = 3;
        double threshold = 0.5;

        FlinkDictionary dictionary = new FlinkDictionary();

        DataSet<IdTokenizedLabelTuple4> tokenizedLabelDataset = dataSet
                .map(new TokenizeMap(nGramSize));

        DataSet<String> nGramCollection = tokenizedLabelDataset
                .flatMap(new CollectTokenFlatMap());

        nGramCollection = nGramCollection.distinct();

        dictionary.add(nGramCollection);

        DataSet<Tuple2<Long, String>> dictionaryDataSet = dictionary.getDictionary();

        DataSet<Tuple3<Integer,Integer,Float>> sortMergeResultDataSet = tokenizedLabelDataset
                .flatMap(new FlinkSortMergeFlatMap(threshold))
                .withBroadcastSet(dictionaryDataSet,"dictionary");

        List<Tuple3<Integer,Integer,Float>> list = sortMergeResultDataSet.collect();

        // Only 4 of the 7 tuples have a similarity value above 0.5
        assertEquals(4,list.size());

        for (Tuple3<Integer,Integer,Float> resultTuple5 : list)
        {
            switch ((int)resultTuple5.getField(0)){
                case 3:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 4:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 5:
                    assertEquals((float) 0.625 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 6:
                    assertEquals((float) (2.0/3) ,(float) resultTuple5.getField(2),0.0001);
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        }
    }

    public void testSortMergeExtendedBiGram() throws Exception{
        //Set the threshold to 0.5 and n-gram digit size to 2
        int nGramSize = 2;
        double threshold = 0.5;

        FlinkDictionary dictionary = new FlinkDictionary();

        DataSet<IdTokenizedLabelTuple4> tokenizedLabelDataset = dataSet
                .map(new TokenizeMap(nGramSize));

        DataSet<String> nGramCollection = tokenizedLabelDataset
                .flatMap(new CollectTokenFlatMap());

        nGramCollection = nGramCollection.distinct();

        dictionary.add(nGramCollection);

        DataSet<Tuple2<Long, String>> dictionaryDataSet = dictionary.getDictionary();

        DataSet<Tuple3<Integer,Integer,Float>> sortMergeResultDataSet = tokenizedLabelDataset
                .flatMap(new FlinkSortMergeFlatMap(threshold))
                .withBroadcastSet(dictionaryDataSet,"dictionary");

        List<Tuple3<Integer,Integer,Float>> list = sortMergeResultDataSet.collect();

        // Only 5 of the 7 tuples have a similarity value above 0.5
        assertEquals(5,list.size());

        for (Tuple3<Integer,Integer,Float> resultTuple5 : list)
        {
            switch ((int)resultTuple5.getField(0)){
                case 3:
                    assertEquals((float) 0.7142 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 4:
                    assertEquals((float) 0.7142 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 5:
                    assertEquals((float) 0.7142 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 6:
                    assertEquals((float) 0.76923 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                case 7:
                    assertEquals((float) 0.545454 ,(float) resultTuple5.getField(2),0.0001);
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        }
    }


}
