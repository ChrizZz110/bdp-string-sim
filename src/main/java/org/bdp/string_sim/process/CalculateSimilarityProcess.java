package org.bdp.string_sim.process;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bdp.string_sim.DataModel;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.transformation.*;
import org.bdp.string_sim.types.IdTokenizedLabelTuple4;
import org.bdp.string_sim.transformation.SortMergeFlatMap;
import org.bdp.string_sim.transformation.StringCompareFlatMap;
import org.bdp.string_sim.transformation.StringCompareTrigramFlatMap;
import org.bdp.string_sim.types.ResultTuple5;
import org.bdp.string_sim.utilities.FileNameHelper;
import org.bdp.string_sim.utilities.FlinkDictionary;

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

        String outputFileName = FileNameHelper.getUniqueFilename(outputDir + "/algo1Result.csv",".csv");
        algo1ResultDataSet.writeAsCsv("file:///" + outputFileName, "\n", ";");

        System.out.println("Finished similarity algorithm: stringCompare.");
    }

    /**
     * Runs the naive string compare algorithm with tokenized labels to calculate the similarity and output result to csv.
     */
    private void runStringCompareNgram()
    {
        System.out.println("Start similarity algorithm: stringCompareNgram.");
        //do algo1 and output a csv file to outputDir
        DataSet<ResultTuple5> algo2ResultDataSet = dataModel.getCrossedIdLabelDataSet()
                .flatMap(new StringCompareTrigramFlatMap(threshold, tokenizeDigits));

        String outputFileName = FileNameHelper.getUniqueFilename(outputDir + "/algo2Result.csv",".csv");
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
        System.out.println("Start similarity algorithm: sortMerge.");
        //do SortMergeAlgo and output a csv file to outputDir

        FlinkDictionary dictionary = new FlinkDictionary();

        DataSet<IdTokenizedLabelTuple4> tokenizedLabelDataset = dataModel.getCrossedIdLabelDataSet()
                .map(new TokenizeMap(tokenizeDigits));

        DataSet<String> nGramCollection = tokenizedLabelDataset
                .flatMap(new CollectTokenFlatMap());

        nGramCollection = nGramCollection.distinct();

        dictionary.add(nGramCollection);

        DataSet<Tuple2<Long, String>> dictionaryDataSet = dictionary.getDictionary();

        DataSet<Tuple3<Integer,Integer,Float>> sortMergeResultIdDataSet = tokenizedLabelDataset
                .flatMap(new FlinkSortMergeFlatMap(this.threshold))
                .withBroadcastSet(dictionaryDataSet,"dictionary");

        DataSet<Tuple2<Tuple3<Integer, Integer, Float>, IdTokenizedLabelTuple4>> joinedDataSet = sortMergeResultIdDataSet
                .join(tokenizedLabelDataset)
                .where(0)
                .equalTo(0);

        DataSet<ResultTuple5> sortMergeResultDataSet = joinedDataSet
                .map(new MapFunction<Tuple2<Tuple3<Integer,Integer,Float>,IdTokenizedLabelTuple4>, Tuple4<Integer,String,Integer,Float>>() {
            @Override
            public Tuple4<Integer, String, Integer, Float> map(Tuple2<Tuple3<Integer, Integer, Float>, IdTokenizedLabelTuple4> input) throws Exception {
                Tuple3<Integer, Integer, Float> tuple3 = input.getField(0);
                IdTokenizedLabelTuple4 tuple4 = input.getField(1);
                return new Tuple4<Integer, String, Integer, Float>(
                        tuple3.getField(0),
                        tuple4.getField(1),
                        tuple3.getField(2),
                        tuple3.getField(3));
            }
        })
                .join(tokenizedLabelDataset)
                .where(2)
                .equalTo(2)
                .map(new MapFunction<Tuple2<Tuple4<Integer,String,Integer,Float>,IdTokenizedLabelTuple4>, ResultTuple5>() {
                    @Override
                    public ResultTuple5 map(Tuple2<Tuple4<Integer, String, Integer, Float>, IdTokenizedLabelTuple4> input) throws Exception {
                        Tuple4<Integer, String, Integer, Float> tuple4A = input.getField(0);
                        IdTokenizedLabelTuple4 tuple4B = input.getField(1);

                        return new ResultTuple5(
                                tuple4A.getField(0),
                                tuple4A.getField(1),
                                tuple4A.getField(2),
                                tuple4B.getField(3),
                                tuple4A.getField(3)
                        );
                    }
                });


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
