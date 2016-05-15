package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.DataModel;

public class MapValue implements MapFunction<Tuple4<Integer,String,String,String>,String> {
    @Override
    public String map(Tuple4<Integer, String, String, String> conceptAttrTuple) throws Exception {
        return conceptAttrTuple.getField(DataModel.CONCEPT_ATTR_COL_PROP_VAL);
    }
}
