package org.bdp.string_sim;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class DataModel {
    public static final int CONCEPT_COL_ENTITY_ID = 0;
    public static final int CONCEPT_COL_URI = 1;
    public static final int CONCEPT_COL_SRC = 2;

    public static final int CONCEPT_ATTR_COL_ENTITY_ID = 0;
    public static final int CONCEPT_ATTR_COL_PROP_NAME = 1;
    public static final int CONCEPT_ATTR_COL_PROP_VAL = 2;
    public static final int CONCEPT_ATTR_COL_PROP_TYPE = 3;

    public static final int LINKS_COL_SRC_ID = 0;
    public static final int LINKS_COL_TRG_ID = 1;

    private DataSet<Tuple3<Integer,String,String>> conceptDataSet;
    private DataSet<Tuple4<Integer,String,String,String>> conceptAttrDataSet;
    private DataSet<Tuple2<Integer,Integer>> linksWithIDsDataSet;

    public DataModel() {
    }

    public DataSet<Tuple3<Integer, String, String>> getConceptDataSet() {
        return conceptDataSet;
    }

    public void setConceptDataSet(DataSet<Tuple3<Integer, String, String>> conceptDataSet) {
        this.conceptDataSet = conceptDataSet;
    }

    public DataSet<Tuple4<Integer, String, String, String>> getConceptAttrDataSet() {
        return conceptAttrDataSet;
    }

    public void setConceptAttrDataSet(DataSet<Tuple4<Integer, String, String, String>> conceptAttrDataSet) {
        this.conceptAttrDataSet = conceptAttrDataSet;
    }

    public DataSet<Tuple2<Integer, Integer>> getLinksWithIDsDataSet() {
        return linksWithIDsDataSet;
    }

    public void setLinksWithIDsDataSet(DataSet<Tuple2<Integer, Integer>> linksWithIDsDataSet) {
        this.linksWithIDsDataSet = linksWithIDsDataSet;
    }
}
