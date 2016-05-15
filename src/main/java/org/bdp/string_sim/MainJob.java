package org.bdp.string_sim;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.bdp.string_sim.transformation.LabelFilter;
import org.bdp.string_sim.importer.Importer;
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
        FilterOperator filteredDataModel = dataModel.getConceptAttrDataSet().filter(new LabelFilter());

        //Map get only the property value of the entity
        MapOperator mapOperator = filteredDataModel.map(new MapValue());

        //to show the result, print it. TODO: Here comes the similarity check
        mapOperator.print();
    }
}
