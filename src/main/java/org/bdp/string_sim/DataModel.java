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
    private DataSet<Tuple4<Integer,String,Integer,String>> crossedIdLabelDataSet;

    public DataModel() {

    }

    /**
     * Getter for conceptDataSet.
     * Returns DataSet<Tuple3<Integer, String, String>>.
     *
     * @return the flink DataSet
     */
    public DataSet<Tuple3<Integer, String, String>> getConceptDataSet() {
        return conceptDataSet;
    }

    /**
     * Setter method.
     *
     * @param conceptDataSet the dataSet of type Tuple3
     */
    public void setConceptDataSet(DataSet<Tuple3<Integer, String, String>> conceptDataSet) {
        this.conceptDataSet = conceptDataSet;
    }

    /**
     * Getter for conceptAttrDataSet.
     * Returns DataSet<Tuple4<Integer, String, String, String>>.
     *
     * @return the flink DataSet
     */
    public DataSet<Tuple4<Integer, String, String, String>> getConceptAttrDataSet() {
        return conceptAttrDataSet;
    }

    /**
     * Setter method.
     *
     * @param conceptAttrDataSet the dataSet of type Tuple4
     */
    public void setConceptAttrDataSet(DataSet<Tuple4<Integer, String, String, String>> conceptAttrDataSet) {
        this.conceptAttrDataSet = conceptAttrDataSet;
    }

    /**
     * Getter for linksWithIDsDataSet.
     * Returns DataSet<Tuple2<Integer, Integer>>.
     *
     * @return the flink DataSet
     */
    public DataSet<Tuple2<Integer, Integer>> getLinksWithIDsDataSet() {
        return linksWithIDsDataSet;
    }

    /**
     * Setter method.
     *
     * @param linksWithIDsDataSet the dataSet of type Tuple2
     */
    public void setLinksWithIDsDataSet(DataSet<Tuple2<Integer, Integer>> linksWithIDsDataSet) {
        this.linksWithIDsDataSet = linksWithIDsDataSet;
    }

    /**
     * Getter for crossedIdLabelDataSet.
     * Returns DataSet<Tuple4<Integer, String, Integer, String>>
     *
     * @return the flink DataSet
     */
    public DataSet<Tuple4<Integer, String, Integer, String>> getCrossedIdLabelDataSet() {
        return crossedIdLabelDataSet;
    }

    /**
     * Setter method.
     *
     * @param crossedIdLabelDataSet the dataSet of type Tuple4
     */
    public void setCrossedIdLabelDataSet(DataSet<Tuple4<Integer, String, Integer, String>> crossedIdLabelDataSet) {
        this.crossedIdLabelDataSet = crossedIdLabelDataSet;
    }
}
