package org.bdp.string_sim.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.transformation.MapIdFromIdValue;
import org.bdp.string_sim.transformation.StrictUpperTriangularMatrixFilter;

public class LabelMerger {

    /**
     * Performs a cartesian product of the given dataSet and filters all tuple to define a strict upper triangular matrix to compare the label values
     *
     * @param conAttrIdsDataSet Dataset with id and label attribute
     * @return the crossed dataSet of type Tuple4<Integer, String, Integer, String>
     */
    public static DataSet<Tuple4<Integer, String, Integer, String>> crossJoinMerge(DataSet<Tuple2<Integer, String>> conAttrIdsDataSet) {

        DataSet<Integer> idsDataSet = conAttrIdsDataSet.map(new MapIdFromIdValue());

        DataSet<Tuple2<Integer, Integer>> crossedLabels = idsDataSet.cross(idsDataSet);

        crossedLabels = crossedLabels.filter(new StrictUpperTriangularMatrixFilter());

        DataSet<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>>> joined1 = crossedLabels.join(conAttrIdsDataSet).where(0).equalTo(0);

        DataSet<Tuple3<Integer, String, Integer>> mapped = joined1.map(new MapFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,String>>, Tuple3<Integer,String,Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>> tuple2Tuple2Tuple2) throws Exception {
                Tuple2<Integer, Integer> integerIntegerTuple2 = tuple2Tuple2Tuple2.getField(0);
                Tuple2<Integer, String> integerStringTuple2 = tuple2Tuple2Tuple2.getField(1);
                return new Tuple3<Integer, String, Integer>(integerStringTuple2.getField(0),integerStringTuple2.getField(1),integerIntegerTuple2.getField(1));
            }
        });

        DataSet<Tuple2<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>>> joined2 = mapped.join(conAttrIdsDataSet).where(2).equalTo(0);

        return joined2.map(new MapFunction<Tuple2<Tuple3<Integer,String,Integer>,Tuple2<Integer,String>>, Tuple4<Integer,String,Integer,String>>() {
            @Override
            public Tuple4<Integer, String, Integer, String> map(Tuple2<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>> tuple3Tuple2Tuple2) throws Exception {
                Tuple3<Integer, String, Integer> integerStringIntegerTuple3 = tuple3Tuple2Tuple2.getField(0);
                Tuple2<Integer, String> integerStringTuple2 = tuple3Tuple2Tuple2.getField(1);

                return new Tuple4<Integer, String, Integer, String>(
                        integerStringIntegerTuple3.getField(0),
                        integerStringIntegerTuple3.getField(1),
                        integerStringIntegerTuple3.getField(2),
                        integerStringTuple2.getField(1)
                );
            }
        });
    }
}
