package org.bdp.string_sim.utilities;

public class DiceMetric {
	
	public static float calculate(int numberTokensA, int numberTokensB, int match) {
		
		float metric = (2*(float)match) / ((float)numberTokensA+(float)numberTokensB);
		return metric;
	}
}
