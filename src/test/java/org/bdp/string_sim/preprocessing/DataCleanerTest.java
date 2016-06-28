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
    
    //csv testdata import
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
    	DataSet<Tuple2<Integer, String>> CleanedTestDataSetTrue = TestDataSet.map(new DataCleaner(true));
    	DataSet<Tuple2<Integer, String>> CleanedTestDataSetFalse = TestDataSet.map(new DataCleaner(false));
    	
    	assertTrue(CleanedTestDataSetTrue.count() == 17);
    	assertTrue(CleanedTestDataSetFalse.count() == 17);
    	List<Tuple2<Integer, String>> collectListTrue = CleanedTestDataSetTrue.collect();
    	List<Tuple2<Integer, String>> collectListFalse = CleanedTestDataSetFalse.collect();
    	
    	//test Mode true - eliminate brackets&content + content after comma
        for(Tuple2<Integer, String> tuple : collectListTrue) {	
        	if ((Integer)tuple.getField(0) >= 0 && (Integer)tuple.getField(0) <= 11) {
        		assertEquals((String)"dresden",(String)tuple.getField(1));
        	}
        }
        
        //test Mode false - leave brackets and content after comma (comma character will still be eliminated)
        for(Tuple2<Integer, String> tuple : collectListFalse) {
        	if ((Integer)tuple.getField(0) >= 12 && (Integer)tuple.getField(0) <= 14) {
        		assertEquals((String)"dresden(sachsen)",(String)tuple.getField(1));
        	}
        	
        	if ((Integer)tuple.getField(0) >= 15 && (Integer)tuple.getField(0) <= 17) {
        		assertEquals((String)"dresdensachsen",(String)tuple.getField(1));
        	}
        }
    }
}