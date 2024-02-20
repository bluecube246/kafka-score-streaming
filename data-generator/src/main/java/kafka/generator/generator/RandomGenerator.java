package kafka.generator.generator;

import java.util.Arrays;
import java.util.List;

import kafka.generator.event.ScoreEvent;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

public class RandomGenerator {

    private static List<String> USER_IDS = Arrays.asList(
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10)
    );

    private static List<String> GAME_IDS = Arrays.asList(
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10),
            RandomStringUtils.randomAlphabetic(10)
    );

    public static ScoreEvent generateScoreEvent(long timestamp) {
        return new ScoreEvent(
                USER_IDS.get(RandomUtils.nextInt(0, 3)),
                GAME_IDS.get(RandomUtils.nextInt(0, 3)),
                RandomUtils.nextInt(0,3),
                timestamp

        );
    }


}
