package de.test.scala;

import de.data.preparation.NoiseInjector;

/**
 * Created by visenger on 05/09/14.
 */
public class MixedPrj {

    public static void main(String[] args) {
        new NoiseInjector().hosp("/Users/visenger/data/HOSP2/input/tmp").noisePercentage(3).inject();
    }
}
