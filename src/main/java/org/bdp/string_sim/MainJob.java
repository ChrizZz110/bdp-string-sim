package org.bdp.string_sim;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.importer.Importer;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.transformation.MapValue;

public class MainJob {

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Importer importer = new Importer();
        DataModel dataModel = new DataModel();

        //read the 3 csv files into the DataModel
        dataModel.setConceptDataSet(importer.getConceptDataSetFromCsv(Importer.CSV_TYPE_PERFECT,env));
        dataModel.setConceptAttrDataSet(importer.getConceptAttrDataSetFromCsv(Importer.CSV_TYPE_PERFECT,env));
        dataModel.setLinksWithIDsDataSet(importer.getLinksWithIDsDataSetFromCsv(Importer.CSV_TYPE_PERFECT,env));

        //Filter only attributes with property name = label
        DataSet<Tuple4<Integer, String, String, String>> filteredDataSet = dataModel.getConceptAttrDataSet().filter(new LabelFilter());

        //Map get only the id and property value of the entity
        DataSet<Tuple2<Integer,String>> idValueDataSet= filteredDataSet.map(new MapValue());

        //Fort testing: to show the result, print it.
        idValueDataSet.print();
    }
}
