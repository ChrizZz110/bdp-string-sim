package org.bdp.string_sim.importer;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.File;

public class Importer {
    public Importer(){

    }

    /**
     * Imports the concept.csv from the given path.
     * @param pathToConceptCsv path to concept.csv
     * @param env the flink execution enviroment
     * @return DataSet<Tuple3> the csv representation as flink dataset
     */
    public DataSet<Tuple3<Integer,String,String>> getConceptDataSetFromCsv(String pathToConceptCsv, ExecutionEnvironment env){
        File conceptFile = new File(pathToConceptCsv);
        //Get concept.csv
        return env.readCsvFile(conceptFile.toString())
                .includeFields("111")
                .fieldDelimiter(";")
                .ignoreInvalidLines()
                .types(Integer.class,String.class,String.class);
    }

    /**
     * Imports the concept_attributes.csv from the given path.
     * @param pathToConceptAttrCsv path to concept_attributes.csv
     * @param env the flink execution enviroment
     * @return DataSet<Tuple4> the csv representation as flink dataset
     */
    public DataSet<Tuple4<Integer,String,String,String>> getConceptAttrDataSetFromCsv(String pathToConceptAttrCsv, ExecutionEnvironment env){
        File conceptAttrFile = new File(pathToConceptAttrCsv);

        //Get concept_attributes.csv
        return env.readCsvFile(conceptAttrFile.toString())
                .includeFields("1111")
                .fieldDelimiter(";")
                .ignoreInvalidLines()
                .types(Integer.class,String.class,String.class,String.class);
    }

    /**
     * Imports the linksWithIDs.csv from the given path.
     * @param pathTolinksWithIDsCsv path to linksWithIDs.csv
     * @param env the flink execution enviroment
     * @return DataSet<Tuple2> the csv representation as flink dataset
     */
    public DataSet<Tuple2<Integer,Integer>> getLinksWithIDsDataSetFromCsv(String pathTolinksWithIDsCsv, ExecutionEnvironment env){
        File linksWithIDsFile = new File(pathTolinksWithIDsCsv);

        //Get linksWithIDs.csv
        return env.readCsvFile(linksWithIDsFile.toString())
                .includeFields("11")
                .fieldDelimiter(";")
                .ignoreInvalidLines()
                .types(Integer.class,Integer.class);
    }

    /**
     * Imports the cleaned and crossed csv from the given path.
     * One entity (one line in the csv) has to be a Tuple4 of type Integer,String,Integer,String
     * which means (Entity ID A),(Label value A),(Entity ID B),(Label value B).
     *
     * @param pathToCsv path to the csv file
     * @param env the flink execution enviroment
     * @return DataSet<Tuple4> the csv representation as flink dataset
     */
    public DataSet<Tuple4<Integer,String,Integer,String>> getMergedIdValueDataSetFromCsv(String pathToCsv, ExecutionEnvironment env){
        File idValueEntitiesFile = new File(pathToCsv);

        //Get concept_attributes.csv
        return env.readCsvFile(idValueEntitiesFile.toString())
                .includeFields("1111")
                .fieldDelimiter(";")
                .ignoreInvalidLines()
                .types(Integer.class,String.class,Integer.class,String.class);
    }
}
