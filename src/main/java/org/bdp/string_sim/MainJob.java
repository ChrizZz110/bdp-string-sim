package org.bdp.string_sim;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.preprocessing.LabelMerger;
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
        DataSet<Integer> conAttrIdsDataSet = filteredDataModel.map(new MapValue());

        conAttrIdsDataSet.print();

        //DataSet<Tuple2<Integer,Integer>> crossedLabels = conAttrIdsDataSet.cross(conAttrIdsDataSet);

        //crossedLabels.print();

        DataSet<Tuple2<Integer,String>> test = env.fromElements(
                new Tuple2<Integer, String>(1,"hund"),
                new Tuple2<Integer, String>(2,"katze"),
                new Tuple2<Integer, String>(3,"maus"),
                new Tuple2<Integer, String>(4,"frosch")
        );
        DataSet<Integer> testInt = env.fromElements(1,2,3,4);

        LabelMerger labelMerger = new LabelMerger(test);

        DataSet<Tuple4<Integer,String,Integer,String>> crossedEntities = labelMerger.crossJoinMerge(testInt);

        //to show the result, print it. TODO: Here comes the similarity check



    }
}

