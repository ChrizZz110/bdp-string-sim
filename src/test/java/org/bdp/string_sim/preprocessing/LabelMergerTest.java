package org.bdp.string_sim.preprocessing;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.transformation.MapIdValue;

import java.util.List;

public class LabelMergerTest extends TestCase {

    private LabelMerger labelMerger;
    private Importer importer;
    private ExecutionEnvironment environment;
    private ClassLoader classLoader;


    public void setUp() throws Exception {
        super.setUp();
        importer = new Importer();
        classLoader = getClass().getClassLoader();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void testCrossJoinMerge() throws Exception{
        DataSet<Tuple4<Integer,String,String,String>> conceptAttrDataSet = importer.getConceptAttrDataSetFromCsv(classLoader.getResource("perfect/concept_attributes.csv").getFile(),environment);
        conceptAttrDataSet = conceptAttrDataSet.filter(new LabelFilter());
        DataSet<Tuple2<Integer, String>> conceptAttrIdValueTupleDataSet = conceptAttrDataSet.map(new MapIdValue());

        assertEquals(31,conceptAttrIdValueTupleDataSet.count());

        DataSet<Tuple4<Integer,String,Integer,String>> result = LabelMerger.crossJoinMerge(conceptAttrIdValueTupleDataSet);
        assertEquals(465,result.count());

        List<Tuple4<Integer, String, Integer, String>> collectList = result.collect();
        for(Tuple4<Integer, String, Integer, String> tuple : collectList)
        {
            assertTrue((int)tuple.getField(0) > (int)tuple.getField(2));
        }

    }
}
