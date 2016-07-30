package org.bdp.string_sim.utilities;

import junit.framework.TestCase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class FlinkDictionaryTest extends TestCase{

    private ExecutionEnvironment environment;

    public void setUp() throws Exception {
        super.setUp();
        environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void testFlinkDictionary() throws Exception {
        DataSet<String> dataSet1 = environment.fromElements(
                "##h","#ha","hau","aus","us#","s##"
        );

        DataSet<String> dataSet2 = environment.fromElements(
                "##m","#ma","mau","aus","us#","s##"
        );

        DataSet<String> dataSet3 = environment.fromElements(
                "##k","#kl","kla","lau","aus","us#","s##"
        );

        FlinkDictionary flinkDictionary = new FlinkDictionary();

        flinkDictionary.add(dataSet1);
        flinkDictionary.add(dataSet2);
        flinkDictionary.add(dataSet3);

        DataSet<Tuple2<Long,String>> dictionary = flinkDictionary.getDictionary();

        List<Tuple2<Long, String>> dictionaryList = dictionary.collect();

        assertEquals(13,dictionaryList.size());

        for (Tuple2<Long,String> dictionaryEntry : dictionaryList)
        {
            String nGram = dictionaryEntry.getField(1);
            long index = dictionaryEntry.getField(0);
            assertEquals(3,nGram.length());
            assertTrue(index >= 0 && index <= 12);
        }
    }
}
