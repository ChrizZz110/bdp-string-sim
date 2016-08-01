package org.bdp.string_sim.utilities;

import junit.framework.TestCase;


public class DiceMetrictTest extends TestCase {
    
    public void setUp() throws Exception {
        super.setUp();
    }
    
    public void testDiceMetric() throws Exception {
    	Integer stringACount = 5;
    	Integer stringBCount = 5;
    	
    	float value1 = DiceMetric.calculate(stringACount, stringBCount, 0);
    	float value2 = DiceMetric.calculate(stringACount, stringBCount, 5);
    	
    	assertTrue(value1 == 0);
    	assertTrue(value2 == 1);
    	assertTrue(value1 >= 0 && value1 <= 1);
    	assertTrue(value2 >= 0 && value2 <= 1);
    }

}