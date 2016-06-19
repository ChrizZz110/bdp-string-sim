package org.bdp.string_sim.processing;

import org.apache.flink.api.java.ExecutionEnvironment;

import junit.framework.TestCase;

public class TokenizerTest extends TestCase {

	private ExecutionEnvironment environment;
    private ClassLoader classLoader;
    
    public void setUp() throws Exception {
        super.setUp();
        //importer = new Importer();
        classLoader = getClass().getClassLoader();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }
    
    public void TestTokenizer()throws Exception {
    	
    }
}
