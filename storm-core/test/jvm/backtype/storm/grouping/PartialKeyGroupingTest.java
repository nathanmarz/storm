package backtype.storm.grouping;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

public class PartialKeyGroupingTest {

    @Test
    public void testChooseTasks() {
        PartialKeyGrouping pkg = new PartialKeyGrouping();
        pkg.prepare(null, null, Lists.newArrayList(0, 1, 2, 3, 4, 5));
        Values message = new Values("key1");
        List<Integer> choice1 = pkg.chooseTasks(0, message);
        assertThat(choice1.size(), is(1));
        List<Integer> choice2 = pkg.chooseTasks(0, message);
        assertThat(choice2, is(not(choice1)));
        List<Integer> choice3 = pkg.chooseTasks(0, message);
        assertThat(choice3, is(not(choice2)));
        assertThat(choice3, is(choice1));
    }
}
