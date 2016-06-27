package org.bdp.string_sim;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.preprocessing.DataCleaner;
import org.bdp.string_sim.process.CalculateSimilarityProcess;
import org.bdp.string_sim.process.CreateCompareCsvProcess;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.transformation.MapIdValue;
import org.bdp.string_sim.utilities.Tokenizer;

import java.util.ArrayList;
import java.util.Arrays;

public class MainJob {

    public static void main(String[] args) throws Exception {
        if(args.length < 1){
            printSyntaxDocumentation();
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
            case "calculateSimilarity":
                try {
                    CalculateSimilarityProcess.main(Arrays.copyOfRange(args, 1,args.length));
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
                break;
            default:
                printSyntaxDocumentation();
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
        //DataSet<Tuple2<Integer,String>> cleanDataSet = idValueDataSet.map(new DataCleaner(true));
        
        //test Tokenizer/Dictionary
        String testString = "Tokenizer";
        String testString2 = "StringValue";
        
        Tokenizer tok = new Tokenizer(4);
        Dictionary dic = new Dictionary();
        
        dic.add(tok.tokenize(testString));
        dic.add(tok.tokenize(testString2));
        System.out.println(dic.getDictionary());
    }

    private static void printSyntaxDocumentation()
    {
        System.out.println("Please use the following syntax:\n"
                +"batch_process_name arg1 arg2 ...\n"
                +"the following processes are available:\n"
                +"\tcreateCompareCsv \n"
                +"\t\tcreates a csv with all attributes to compare including their ids\n"
                +"\t\targuments: path/to/concept_attribute.csv path/to/output.csv\n"
                +"\n\tcalculateSimilarity \n"
                +"\t\tcalculates a similarity value of each entity tuple in the input csv file\n"
                +"\t\targuments: path/to/cleanAndMergedIdLabels.csv path/to/output/directory\n"
                +"\n\tdefault \n"
                +"\t\timports the concept_attribute.csv, filters only label attributes, maps id and value and prints it out\n"
                +"\t\targuments: path/to/concept_attribute.csv \n"
        );
    }
}
