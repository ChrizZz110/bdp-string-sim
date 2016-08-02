package org.bdp.string_sim.utilities;

public class DiceMetric {
	/**
     * This Function calculates the dicemetric based on given numbers of token for both strings and the number of matches between them
     * @param numberTokensA number of tokens for string A
     * @param numberTokensB number of tokens for string B
     * @param match number of matching tokens between string A and string B 
     * @return calculated metric of type float
     */
	public static float calculate(int numberTokensA, int numberTokensB, int match) {
		return (2*(float)match) / (numberTokensA+numberTokensB);
	}
}
