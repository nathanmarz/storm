package storm.kafka;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Date: 12/01/2014
 * Time: 18:09
 */
public class KafkaErrorTest {

    @Test
    public void getError() {
        assertThat(KafkaError.getError(0), is(equalTo(KafkaError.NO_ERROR)));
    }

    @Test
    public void offsetMetaDataTooLarge() {
        assertThat(KafkaError.getError(12), is(equalTo(KafkaError.OFFSET_METADATA_TOO_LARGE)));
    }

    @Test
    public void unknownNegative() {
        assertThat(KafkaError.getError(-1), is(equalTo(KafkaError.UNKNOWN)));
    }

    @Test
    public void unknownPositive() {
        assertThat(KafkaError.getError(75), is(equalTo(KafkaError.UNKNOWN)));
    }

    @Test
    public void unknown() {
        assertThat(KafkaError.getError(13), is(equalTo(KafkaError.UNKNOWN)));
    }
}
