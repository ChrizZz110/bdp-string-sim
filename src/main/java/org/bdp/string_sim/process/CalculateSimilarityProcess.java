package org.bdp.string_sim.process;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bdp.string_sim.DataModel;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.transformation.SortMergeFlatMap;
import org.bdp.string_sim.transformation.StringCompareMap;
import org.bdp.string_sim.types.ResultTuple5;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Class for processing the similarity calculation.
 */
public class CalculateSimilarityProcess {

    /**
     * Run the similarity calculation. inputFile has to be the path to cleaned and merged csv file.
     * One entity (one line in the csv) has to be a Tuple4 of type Integer,String,Integer,String
     * which means (Entity ID A),(Label value A),(Entity ID B),(Label value B).
     *
     * @param algorithmArray An array of strings which specifies the algorithms which should be used for the similarity check
     * @param inputCsv The path to the cleaned and merged csv file.
     * @param outputDir Path to a directory, where the resulting csv files can be placed
     * @param threshold Only tuples with a similarity value above (>=) the threshold will be collected. The threshold has to be between 0 and 1.
     * @param tokenizeDigits Used by the tokenizer. The size of a n-gram.
     * @throws Exception
     */
    private void run(String[] algorithmArray, String inputCsv, String outputDir, double threshold, int tokenizeDigits) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Importer importer = new Importer();
        DataModel dataModel = new DataModel();

        //Check input file
        File idValueEntitiesFile = new File(inputCsv);
        if(!idValueEntitiesFile.exists() || !idValueEntitiesFile.isFile()){
            //File does not exist.
            throw new FileNotFoundException("The file " + inputCsv + " doesn't exist. Please make sure the path is correct.");
        }

        //Check path for output file
        File outputDirFile = new File(outputDir);
        if(!outputDirFile.exists() || !outputDirFile.isDirectory()){
            //Directory does not exist. Create it.
            if(outputDirFile.mkdir()) {
                System.out.println("Directory Created");
            } else {
                throw new FileNotFoundException("The directory " + outputDir + " doesn't exist and can not be created. Please make sure the path is correct.");
            }
        }

        //import data
        dataModel.setCrossedIdLabelDataSet(
                importer.getMergedIdValueDataSetFromCsv(inputCsv, env)
        );

        for (String algorithmName: algorithmArray){
            switch (algorithmName){
                case "stringCompare":
                    System.out.println("Start similarity algorithm: stringCompare.");
                    //do algo1 and output a csv file to outputDir
                    DataSet<ResultTuple5> algo1ResultDataSet = dataModel.getCrossedIdLabelDataSet().flatMap(new StringCompareMap(threshold > 0.0));
                    algo1ResultDataSet.writeAsCsv("file:///" + outputDir + "/algo1Result.csv","\n",";");
                    System.out.println("Finished similarity algorithm: stringCompare.");
                    break;
                case "stringCompareNgram":

                    //TODO: Put ngram transformation here

                    break;
                case "sortMerge":
                    System.out.println("Start similarity algorithm: sortMerge.");
                    //do SortMergeAlgo and output a csv file to outputDir
                    DataSet<ResultTuple5> sortMergeResultDataSet = dataModel.getCrossedIdLabelDataSet().flatMap(new SortMergeFlatMap(threshold,tokenizeDigits));
                    sortMergeResultDataSet.writeAsCsv("file:///" + outputDir + "/sortMergeResult.csv","\n",";");
                    System.out.println("Finished similarity algorithm: sortMerge.");
                    break;
                case "simmetrics":

                    break;
                default:
                    throw new RuntimeException("One of the algorithm names in the program arguments is invalid. " +
                            "Choose one or more of the following: stringCompare,stringCompareNgram,sortMerge,simmetrics");
            }
        }

        env.execute("CalculateSimilarityProcess");
    }

    /**
     * Entry point to start the process.
     *
     * @param parameters Flink ParameterTool object
     */
    public static void main(ParameterTool parameters) throws Exception {
        CalculateSimilarityProcess csp = new CalculateSimilarityProcess();
        String[] algorithmArray = parameters.getRequired("algorithms").split(",");
        csp.run(
                algorithmArray,
                parameters.getRequired("inputCsv"),
                parameters.getRequired("outputDir"),
                parameters.getDouble("threshold", 0.0),
                parameters.getInt("tokenizeDigits", 3)
        );
    }
}
