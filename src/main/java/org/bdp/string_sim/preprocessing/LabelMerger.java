package org.bdp.string_sim.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bdp.string_sim.transformation.StrictUpperTriangularMatrixFilter;

public class LabelMerger {
    private DataSet<Tuple2<Integer,String>> conceptAttrTupleDataSet;

    public LabelMerger(DataSet<Tuple2<Integer, String>> conceptAttrTupleDataSet) {
        this.conceptAttrTupleDataSet = conceptAttrTupleDataSet;
    }

    public DataSet<Tuple4<Integer,String,Integer,String>> crossJoinMerge(DataSet<Integer> conAttrIdsDataSet) throws Exception {

        DataSet<Tuple2<Integer,Integer>> crossedLabels = conAttrIdsDataSet.cross(conAttrIdsDataSet);

        crossedLabels = crossedLabels.filter(new StrictUpperTriangularMatrixFilter());

        DataSet<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,String>>> joined1 = crossedLabels.join(conceptAttrTupleDataSet).where(0).equalTo(0);

        DataSet<Tuple3<Integer, String, Integer>> mapped = joined1.map(new MapFunction<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,String>>, Tuple3<Integer,String,Integer>>() {
            @Override
            public Tuple3<Integer, String, Integer> map(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, String>> tuple2Tuple2Tuple2) throws Exception {
                Tuple2<Integer, Integer> integerIntegerTuple2 = tuple2Tuple2Tuple2.getField(0);
                Tuple2<Integer, String> integerStringTuple2 = tuple2Tuple2Tuple2.getField(1);
                return new Tuple3<Integer, String, Integer>(integerStringTuple2.getField(0),integerStringTuple2.getField(1),integerIntegerTuple2.getField(1));
            }
        });

        DataSet<Tuple2<Tuple3<Integer, String, Integer>,Tuple2<Integer,String>>> joined2 = mapped.join(conceptAttrTupleDataSet).where(2).equalTo(0);

        DataSet<Tuple4<Integer,String,Integer,String>> result = joined2.map(new MapFunction<Tuple2<Tuple3<Integer,String,Integer>,Tuple2<Integer,String>>, Tuple4<Integer,String,Integer,String>>() {
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

        result.print();

        return result;
    }


}
