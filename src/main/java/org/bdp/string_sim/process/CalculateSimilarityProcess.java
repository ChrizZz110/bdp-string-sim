package org.bdp.string_sim.process;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.bdp.string_sim.DataModel;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.transformation.StringCompareMap;
import org.bdp.string_sim.transformation.StringCompareTrigram;
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
     * @param inputFile The path to the cleaned and merged csv file.
     * @param outputDirectory Path to a directory, where the resulting csv files can be placed
     * @throws Exception
     */
    private void run(String inputFile,String outputDirectory) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Importer importer = new Importer();
        DataModel dataModel = new DataModel();

        //Check input file
        File idValueEntitiesFile = new File(inputFile);
        if(!idValueEntitiesFile.exists() || !idValueEntitiesFile.isFile()){
            //File does not exist.
            throw new FileNotFoundException("The file " + inputFile + " doesn't exist. Please make sure the path is correct.");
        }

        //Check path for output file
        File outputDir = new File(outputDirectory);
        if(!outputDir.exists() || !outputDir.isDirectory()){
            //Directory does not exist. Create it.
            if(outputDir.mkdir()) {
                System.out.println("Directory Created");
            } else {
                throw new FileNotFoundException("The directory " + outputDirectory + " doesn't exist and can not be created. Please make sure the path is correct.");
            }
        }

        //import data
        dataModel.setCrossedIdLabelDataSet(
                importer.getMergedIdValueDataSetFromCsv(inputFile, env)
        );

        //do algo1 and output a csv file to outputDir
        DataSet<ResultTuple5> algo1ResultDataSet = dataModel.getCrossedIdLabelDataSet().flatMap(new StringCompareMap(false));
        algo1ResultDataSet.writeAsCsv("file:///" + outputDir + "/algo1Result.csv","\n",";");

        //do algo2 and output a csv file to outputDir
        //DataSet<ResultTuple5> algo2ResultDataSet = dataModel.getCrossedIdLabelDataSet().flatMap(new StringCompareTrigram());
        //algo2ResultDataSet.writeAsCsv("file:///" + outputDir + "/algo2Result.csv","\n",";");

        //do algo3 and output a csv file to outputDir

        //do algo4 and output a csv file to outputDir

        env.execute("CalculateSimilarityProcess");
    }

    /**
     * Entry point to start the process.
     *
     * @param args 1st parameter is used as path to config file
     */
    public static void main(String[] args) throws Exception {
        CalculateSimilarityProcess csp = new CalculateSimilarityProcess();
        csp.run(args[0],args[1]);
    }
}
