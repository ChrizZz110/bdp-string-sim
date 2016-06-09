package org.bdp.string_sim.transformation;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.importer.Importer;

public class LabelFilterTest extends TestCase {
    private Importer importer;
    private ExecutionEnvironment environment;

    public void setUp() throws Exception {
        super.setUp();
        importer = new Importer();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * Tests the Label Filter class
     *
     * @throws Exception
     */
    public void testLabelFilter() throws Exception{
        DataSet<Tuple4<Integer,String,String,String>> dataSet = importer.getConceptAttrDataSetFromCsv(Importer.CSV_TYPE_PERFECT,environment);
        assertEquals(142,dataSet.count());
        dataSet = dataSet.filter(new LabelFilter());
        assertEquals(31,dataSet.count());
    }
}
