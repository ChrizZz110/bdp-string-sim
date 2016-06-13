package org.bdp.string_sim.preprocessing;

import java.io.File;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import junit.framework.TestCase;

public class DataCleanerTest extends TestCase {
    private ExecutionEnvironment environment;
    private ClassLoader classLoader;

    public void setUp() throws Exception {
        super.setUp();
        environment = ExecutionEnvironment.getExecutionEnvironment();
        classLoader = getClass().getClassLoader();
    }
    
    //csv testdata import for Tuple2<Integer,String>
    public DataSet<Tuple2<Integer,String>> getDataCleanerTestDataFromCsv(){
        File testData = new File(classLoader.getResource("iss4DataCleanerTestData.csv").getFile());

        return environment.readCsvFile(testData.toString())
                .includeFields("11")
                .fieldDelimiter(";")
                .ignoreInvalidLines()
                .types(Integer.class,String.class);
    }
    
    public void testDataCleaner() throws Exception {
    	DataSet<Tuple2<Integer, String>> TestDataSet = this.getDataCleanerTestDataFromCsv();
    	DataSet<Tuple2<Integer, String>> CleanedTestDataSet = TestDataSet.map(new DataCleaner());
    	
    	//CleanedTestDataSet.print();
    	
    	List<Tuple2<Integer, String>> collectList = CleanedTestDataSet.collect();
        for(Tuple2<Integer, String> tuple : collectList)
        {	
        	
            assertEquals((String)"dresden",(String)tuple.getField(1));
        }
    }
}