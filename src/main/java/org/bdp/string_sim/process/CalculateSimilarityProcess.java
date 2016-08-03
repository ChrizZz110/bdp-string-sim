package org.bdp.string_sim.process;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bdp.string_sim.DataModel;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.transformation.*;
import org.bdp.string_sim.types.*;
import org.bdp.string_sim.transformation.SortMergeFlatMap;
import org.bdp.string_sim.transformation.StringCompareFlatMap;
import org.bdp.string_sim.utilities.FileNameHelper;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Class for processing the similarity calculation.
 */
public class CalculateSimilarityProcess {

    private DataModel dataModel;
    private double threshold;
    private String outputDir;
    private int tokenizeDigits;
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
        dataModel = new DataModel();
        this.threshold = threshold;
        this.outputDir = outputDir;
        this.tokenizeDigits = tokenizeDigits;
        Importer importer = new Importer();

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

                    runStringCompare();

                    break;
                case "stringCompareNgram":
                    runStringCompareNgram();

                    break;
                case "sortMerge":

                    runSortMerge();

                    break;
                case "flinkSortMerge":

                    runFlinkSortMerge();

                    break;
                case "simmetrics":

                    runSimmetrics();

                    break;
                default:
                    throw new RuntimeException("One of the algorithm names in the program arguments is invalid. " +
                            "Choose one or more of the following: stringCompare,stringCompareNgram,flinkSortMerge,sortMerge,simmetrics");
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

    /**
     * Runs the naive string compare algorithm to calculate the similarity and output result to csv.
     */
    private void runStringCompare()
    {
        System.out.println("Start similarity algorithm: stringCompare.");
        //do algo1 and output a csv file to outputDir
        DataSet<ResultTuple5> algo1ResultDataSet = dataModel.getCrossedIdLabelDataSet()
                .flatMap(new StringCompareFlatMap(this.threshold > 0.0));

        String outputFileName = FileNameHelper.getUniqueFilename(outputDir + "/stringCompareResult.csv",".csv");
        algo1ResultDataSet.writeAsCsv("file:///" + outputFileName, "\n", ";");

        System.out.println("Finished similarity algorithm: stringCompare.");
    }

    /**
     * Runs the naive string compare algorithm with tokenized labels to calculate the similarity and output result to csv.
     */
    private void runStringCompareNgram()
    {
        System.out.println("Start similarity algorithm: stringCompareNgram.");

        DataSet<Tuple2<Integer,String>> idLabelTuple2A = dataModel.getCrossedIdLabelDataSet()
                .distinct(0)
                .project(0,1);

        DataSet<Tuple2<Integer,String>> idLabelTuple2 = idLabelTuple2A
                .union(
                        dataModel.getCrossedIdLabelDataSet()
                                .distinct(2)
                                .project(2,3)
                )
                .distinct(0);

        DataSet<IdTokenizedLabelTuple2> idTokenizedLabelDataSet = idLabelTuple2
                .map(new IdTokenizedLabelMap(this.tokenizeDigits));

        DataSet<ResultTuple5> algo2ResultDataSet = dataModel.getCrossedIdLabelDataSet()
                .flatMap(new StringCompareNgramRichFlatMap(threshold))
                .withBroadcastSet(idTokenizedLabelDataSet,"idTokenizedLabelCollection");

        String outputFileName = FileNameHelper.getUniqueFilename(outputDir + "/stringCompareNgramResult.csv",".csv");
        algo2ResultDataSet.writeAsCsv("file:///" + outputFileName, "\n", ";");

        System.out.println("Finished similarity algorithm: stringCompareNgram.");
    }

    /**
     * Runs the sort merge algorithm to calculate the similarity and output result to csv.
     */
    private void runSortMerge() {
        System.out.println("Start similarity algorithm: sortMerge.");
        //do SortMergeAlgo and output a csv file to outputDir
        DataSet<ResultTuple5> sortMergeResultDataSet = dataModel.getCrossedIdLabelDataSet()
                .flatMap(new SortMergeFlatMap(threshold,tokenizeDigits));

        String outputFileName = FileNameHelper.getUniqueFilename(outputDir + "/sortMergeResult.csv",".csv");
        sortMergeResultDataSet.writeAsCsv("file:///" + outputFileName, "\n", ";");

        System.out.println("Finished similarity algorithm: sortMerge.");
    }

    /**
     * Runs a plain flink sort merge algorithm to calculate the similarity and output result to csv.
     */
    private void runFlinkSortMerge()
    {
        System.out.println("Start similarity algorithm: flinkSortMerge.");

        DataSet<Tuple2<Integer,String>> idLabelTuple2A = dataModel.getCrossedIdLabelDataSet()
                .distinct(0)
                .project(0,1);

        DataSet<Tuple2<Integer,String>> idLabelTuple2 = idLabelTuple2A
                .union(
                        dataModel.getCrossedIdLabelDataSet()
                                .distinct(2)
                                .project(2,3)
                )
                .distinct(0);

        DataSet<IdTokenizedLabelTuple2> idTokenizedLabelDataSet = idLabelTuple2
                .map(new IdTokenizedLabelMap(this.tokenizeDigits));

        DataSet<String> distinctTokenDataSet = idTokenizedLabelDataSet
                .flatMap(new CollectTokenFromTuple2FlatMap())
                .distinct();

        DataSet<Tuple2<Long, String>> flinkDictionary = DataSetUtils.zipWithUniqueId(distinctTokenDataSet);

        DataSet<Tuple2<Integer,Long[]>> idTranslatedTokenDataSet = idTokenizedLabelDataSet
                .map(new TranslateTokensFromTuple2Map())
                .withBroadcastSet(flinkDictionary,"flinkDictionary");

        DataSet<ResultTuple5> sortMergeResultDataSet = dataModel.getCrossedIdLabelDataSet()
                .flatMap(new FlinkSortMergeRichFlatMap(this.threshold))
                .withBroadcastSet(idTranslatedTokenDataSet,"translatedTokenDictionary");

        String outputFileName = FileNameHelper.getUniqueFilename(outputDir + "/flinkSortMergeResult.csv",".csv");
        sortMergeResultDataSet.writeAsCsv("file:///" + outputFileName, "\n", ";");

        System.out.println("Finished similarity algorithm: sortMerge.");
    }

    /**
     * Runs simmetrics algorithm to calculate the similarity and output result to csv.
     */
    private void runSimmetrics()
    {
        System.out.println("Start similarity algorithm: simmetrics.");

        DataSet<ResultTuple5> simmetricsResultDataSet = dataModel.getCrossedIdLabelDataSet()
                .flatMap(new SimmetricsFlatMap(threshold,tokenizeDigits));

        String outputFileName = FileNameHelper.getUniqueFilename(outputDir + "/simmetricsResult.csv",".csv");
        simmetricsResultDataSet.writeAsCsv("file:///" + outputFileName, "\n", ";");

        System.out.println("Finished similarity algorithm: simmetrics.");
    }
}
