package kafka.generator.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class ScoreEvent {
    private String userId;
    private String gameId;
    private int score;
    private long timestamp;
}
