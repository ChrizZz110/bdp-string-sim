package org.bdp.string_sim.process;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bdp.string_sim.DataModel;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.preprocessing.DataCleaner;
import org.bdp.string_sim.preprocessing.LabelMerger;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.transformation.MapIdValue;

public class CreateCompareCsvProcess {

    /**
     * run conversion process
     * @param input path to concept_attributes.csv
     * @param output path to output csv
     * @throws Exception
     */
    private void run(String input,String output) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Importer importer = new Importer();
        DataModel dataModel = new DataModel();

        //read the csv files into the DataModel
        dataModel.setConceptAttrDataSet(importer.getConceptAttrDataSetFromCsv(input,env));

        DataSet<Tuple2<Integer,String>> cleanIdValueDataSet = dataModel.getConceptAttrDataSet()
                //Filter only attributes with property name = label
                .filter(new LabelFilter())

                //Map get only the id and property value of the entity
                .map(new MapIdValue())

                //Apply the data cleaner
                .map(new DataCleaner());;


        //Cross it (Cartesian Product) , join ids with values
        DataSet<Tuple4<Integer, String, Integer, String>> comparisonDataSet = LabelMerger.crossJoinMerge(cleanIdValueDataSet);

        //Put it in a csv file.
        comparisonDataSet.writeAsCsv("file:///" + output,"\n",";");

        env.execute("CreateCompareCsvProcess");
    }

    /**
     * entry point
     *
     * @param parameters Flink ParameterTool object
     */
    public static void main(ParameterTool parameters) throws Exception {
        CreateCompareCsvProcess cccp = new CreateCompareCsvProcess();
        cccp.run(parameters.getRequired("inputCsv"),parameters.getRequired("outputCsv"));
    }
}
