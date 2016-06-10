package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.DataModel;

public class MapIdValue implements MapFunction<Tuple4<Integer,String,String,String>,Tuple2<Integer,String>> {

    /**
     * This MapFunction class is applicable to the flink transformation map(). It maps a Tuple4 to a Tuple2 while ignoring the second and fourth element of the tuple.
     * Example: {1,'label',Leipzig,string} is mapped to {1,Leipzig}
     *
     * @param conceptAttrTuple the input tuple from the concept_attribute dataset of type Tuple4<Integer, String, String, String>
     * @return Tuple2<> of type Integer and String => the id and the property value
     * @throws Exception
     */
    @Override
    public Tuple2<Integer,String> map(Tuple4<Integer, String, String, String> conceptAttrTuple) throws Exception {
        return new Tuple2<>(conceptAttrTuple.getField(DataModel.CONCEPT_ATTR_COL_ENTITY_ID),conceptAttrTuple.getField(DataModel.CONCEPT_ATTR_COL_PROP_VAL));
    }
}
