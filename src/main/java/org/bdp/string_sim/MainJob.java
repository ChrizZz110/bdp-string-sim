package org.bdp.string_sim;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.preprocessing.DataCleaner;
import org.bdp.string_sim.process.CreateCompareCsvProcess;
import org.bdp.string_sim.process.Tokenizer;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.transformation.MapIdValue;

import java.io.ObjectInputStream.GetField;
import java.util.Arrays;

public class MainJob {

    public static void main(String[] args) throws Exception {
        if(args.length < 1){
            System.out.println("Please use the following syntax:\n"
                    +"batch_process_name arg1 arg2 ...\n"
                    +"the following processes are available:\n"
                    +"\tcreateCompareCsv \n"
                    +"\t\tcreates a csv with all attributes to compare including their ids\n"
                    +"\t\targuments: path/to/concept_attribute.csv path/to/output.csv\n"
                    +"\n\tdefault \n"
                    +"\t\timports the concept_attribute.csv, filters only label attributes, maps id and value and prints it out\n"
                    +"\t\targuments: path/to/concept_attribute.csv \n"
            );
            return;
        }

        switch (args[0]){
            case "createCompareCsv":
                try {
                    CreateCompareCsvProcess.main(Arrays.copyOfRange(args, 1,args.length));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "default":
                try {
                    runDefault(Arrays.copyOfRange(args, 1,args.length));
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }


    }

    private static void runDefault(String[] args) throws Exception
    {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Importer importer = new Importer();
        DataModel dataModel = new DataModel();

        //read the csv files into the DataModel
        dataModel.setConceptAttrDataSet(importer.getConceptAttrDataSetFromCsv(args[0],env));

        //Filter only attributes with property name = label
        DataSet<Tuple4<Integer, String, String, String>> filteredDataSet = dataModel.getConceptAttrDataSet().filter(new LabelFilter());

        //Map get only the id and property value of the entity
        DataSet<Tuple2<Integer,String>> idValueDataSet= filteredDataSet.map(new MapIdValue());
        
        //clean data of property value
        DataSet<Tuple2<Integer,String>> cleanDataSet = idValueDataSet.map(new DataCleaner());
        //cleanDataSet.print();
        
        //DataSet<String> testString = env.fromElements("Tokenizer");
        String testString = "Tokenizer";
        

    }
}
