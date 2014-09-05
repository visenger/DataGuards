package de.test.scala;

import de.data.preparation.NoiseInjector;

/**
 * Created by visenger on 05/09/14.
 */
public class MixedPrj {

    public static void main(String[] args) {
        new NoiseInjector().hosp("test/path/to/data/").noisePercentage(3).inject();
    }
}
