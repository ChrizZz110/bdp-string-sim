package org.bdp.string_sim.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.DataModel;

public class LabelFilter implements FilterFunction<Tuple4<Integer, String, String, String>> {
    public boolean filter(Tuple4<Integer, String, String, String> conceptAttrTuple) throws Exception {
        return conceptAttrTuple.getField(DataModel.CONCEPT_ATTR_COL_PROP_NAME).equals("label");
    }
}

