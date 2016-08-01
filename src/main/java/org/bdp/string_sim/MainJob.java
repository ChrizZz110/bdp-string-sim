package org.bdp.string_sim;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.preprocessing.DataCleaner;
import org.bdp.string_sim.process.CalculateSimilarityProcess;
import org.bdp.string_sim.process.CreateCompareCsvProcess;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.transformation.MapIdValue;
import org.bdp.string_sim.utilities.DiceMetric;
import org.bdp.string_sim.utilities.Dictionary;
import org.bdp.string_sim.utilities.Tokenizer;

public class MainJob {

    public static void main(String[] args) throws Exception
    {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        if(parameters.getNumberOfParameters() < 1){
            printSyntaxDocumentation();
            return;
        }

        switch (parameters.get("process", "none")){
            case "createCompareCsv":
                try {
                    CreateCompareCsvProcess.main(parameters);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "calculateSimilarity":
                try {
                    CalculateSimilarityProcess.main(parameters);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "default":
                try {
                    runDefault(parameters);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
                printSyntaxDocumentation();
        }
    }

    private static void runDefault(ParameterTool parameters) throws Exception
    {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Importer importer = new Importer();
        DataModel dataModel = new DataModel();

        //read the csv files into the DataModel
        dataModel.setConceptAttrDataSet(importer.getConceptAttrDataSetFromCsv(parameters.getRequired("inputCsv"),env));

        //Filter only attributes with property name = label
        DataSet<Tuple4<Integer, String, String, String>> filteredDataSet = dataModel.getConceptAttrDataSet().filter(new LabelFilter());

        //Map get only the id and property value of the entity
        DataSet<Tuple2<Integer,String>> idValueDataSet= filteredDataSet.map(new MapIdValue());

        //clean data of property value
        DataSet<Tuple2<Integer,String>> cleanDataSet = idValueDataSet.map(new DataCleaner(true));

        cleanDataSet.print();

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
        System.out.println("Please use the syntax given in the gitHub description: https://github.com/ChrizZz110/bdp-string-sim \n" +
                "Examples:\n" +
                "\t--process default --inputCsv path/to/concept_attribute.csv\n\n" +
                "\t--process createCompareCsv --inputCsv path/to/concept_attribute.csv --outputCsv path/to/output.csv\n\n" +
                "\t--process calculateSimilarity --inputCsv path/to/crossMerged.csv " +
                "--outputDir path/to/output/directory --algorithms stringCompare,stringCompareNgram,flinkSortMerge,sortMerge,simmetrics" +
                "--threshold 0.6 --tokenizeDigits 2\n"
        );
    }
}
