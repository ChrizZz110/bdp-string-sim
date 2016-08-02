package org.bdp.string_sim.transformation;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.bdp.string_sim.types.IdTokenizedLabelTuple6;
import org.bdp.string_sim.types.ResultTuple5;
import org.bdp.string_sim.utilities.DiceMetric;

public class StringCompareNgramFlatMap implements FlatMapFunction<IdTokenizedLabelTuple6, ResultTuple5> {

    private double thresholdMatch = 0.0;

    public StringCompareNgramFlatMap() {
    }

    /**
     * This constructor creates an StringCompareNgramFlatMap instance.
     *
     * @param thresholdMatch only collects matches equal or above this threshold. threshold must be between 0.0 and 1.0.
     */
    public StringCompareNgramFlatMap(double thresholdMatch) {
        if (thresholdMatch >= 0.0 && thresholdMatch <= 1.0) {
            this.thresholdMatch = thresholdMatch;
        }
    }

    /**
     * This flatMap function compares tokenized strings in field 2 and 5 of the input tuple6 compare it against
     * each other. According to matching tokens a diceMetric is calculated and added in a ResultTuple5. Matches will be only be saved if they
     * have a sufficient diceMetric, according to propterty thresholdMatch.
     *
     * @param input     the input tuple6:
     *                  Field 0: the id of label A
     *                  Field 1: the label A
     *                  Field 2: the tokenized label A as string[]
     *                  Field 3: the id of label B
     *                  Field 4: the label B
     *                  Field 5: the tokenized label B as string[]
     * @param collector the collector which collects all result tuple of type ResultTuple5
     * @throws Exception
     */
    @Override
    public void flatMap(IdTokenizedLabelTuple6 input, Collector<ResultTuple5> collector) throws Exception {

        ArrayList<String> stringListA = new ArrayList<>(Arrays.asList(input.getField(2)));
        ArrayList<String> stringListB = new ArrayList<>(Arrays.asList(input.getField(5)));

        Integer sizeListA = stringListA.size();
        Integer sizeListB = stringListB.size();
        Integer match = 0;

        for (String a : stringListA) {
            for (String b : stringListB) {
                if (a.equals(b)) {
                    match++;
                    // only one match per token due to integrity of diceMetric
                    stringListB.remove(b);
                    break;
                }
            }
        }

        float diceMetric = DiceMetric.calculate(sizeListA, sizeListB, match);

        if (diceMetric >= thresholdMatch) {
            collector.collect(
                    new ResultTuple5(
                            input.getField(0),
                            input.getField(1),
                            input.getField(3),
                            input.getField(4),
                            diceMetric
                    )
            );
        }
    }
}
