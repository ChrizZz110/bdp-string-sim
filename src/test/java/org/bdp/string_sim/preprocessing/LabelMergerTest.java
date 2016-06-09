package org.bdp.string_sim.preprocessing;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.transformation.MapId;
import org.bdp.string_sim.transformation.MapValue;

public class LabelMergerTest extends TestCase {

    private LabelMerger labelMerger;
    private Importer importer;
    private ExecutionEnvironment environment;
    private DataSet<Tuple2<Integer, String>> conceptAttrIdValueTupleDataSet;
    private DataSet<Tuple4<Integer,String,String,String>> conceptAttrDataSet;

    public void setUp() throws Exception {
        super.setUp();
        importer = new Importer();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void testCrossJoinMerge() throws Exception{
        conceptAttrDataSet = importer.getConceptAttrDataSetFromCsv(Importer.CSV_TYPE_PERFECT,environment);
        conceptAttrDataSet = conceptAttrDataSet.filter(new LabelFilter());
        conceptAttrIdValueTupleDataSet = conceptAttrDataSet.map(new MapValue());

        assertEquals(31,conceptAttrIdValueTupleDataSet.count());

        labelMerger = new LabelMerger(conceptAttrIdValueTupleDataSet);
        DataSet<Integer> idDataSet = conceptAttrDataSet.map(new MapId());

        DataSet<Tuple4<Integer,String,Integer,String>> result = labelMerger.crossJoinMerge(idDataSet);

        assertEquals(465,result.count());
    }
}
