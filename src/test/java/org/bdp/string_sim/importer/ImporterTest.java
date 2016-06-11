package org.bdp.string_sim.importer;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class ImporterTest extends TestCase {
    private Importer importer;
    private ExecutionEnvironment environment;
    private ClassLoader classLoader;

    public void setUp() throws Exception {
        super.setUp();
        importer = new Importer();
        environment = ExecutionEnvironment.getExecutionEnvironment();
        classLoader = getClass().getClassLoader();
    }

    /**
     * Test the import of the concept.csv
     * @throws Exception
     */
    public void testGetConceptDataSetFromCsv() throws Exception {
        DataSet dataSet =  importer.getConceptDataSetFromCsv(classLoader.getResource("perfect/concept.csv").getFile(),environment);
        assertEquals((long)30,dataSet.count());
    }

    /**
     * Test the import of the concept_attributes.csv
     * @throws Exception
     */
    public void testGetConceptAttrDataSetFromCsv() throws Exception {
        DataSet dataSet = importer.getConceptAttrDataSetFromCsv(classLoader.getResource("perfect/concept_attributes.csv").getFile(),environment);
        assertEquals((long) 142,dataSet.count());
    }

    /**
     * Test the import of the linksWithIDs.csv
     * @throws Exception
     */
    public void testGetLinksWithIDsDataSetFromCsv() throws Exception {
        DataSet dataSet = importer.getLinksWithIDsDataSetFromCsv(classLoader.getResource("perfect/linksWithIDs.csv").getFile(),environment);
        assertEquals((long) 5627 , dataSet.count());
    }

}