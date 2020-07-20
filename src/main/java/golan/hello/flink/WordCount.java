package golan.hello.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.fromElements(
				""+			//https://en.wikipedia.org/wiki/To_be,_or_not_to_be
						"" +
						"To be, or not to be, that is the question:\n" +
						"Whether 'tis nobler in the mind to suffer\n" +
						"The slings and arrows of outrageous fortune,\n" +
						"Or to take Arms against a Sea of troubles,\n" +
						"And by opposing end them: to die, to sleep;\n" +
						"No more; and by a sleep, to say we end\n" +
						"The heart-ache, and the thousand natural shocks\n" +
						"That Flesh is heir to? 'Tis a consummation\n" +
						"Devoutly to be wished. To die, to sleep,\n" +
						"To sleep, perchance to Dream; aye, there's the rub,\n" +
						"For in that sleep of death, what dreams may come,\n" +
						"When we have shuffled off this mortal coil,\n" +
						"Must give us pause. There's the respect\n" +
						"That makes Calamity of so long life:\n" +
						"For who would bear the Whips and Scorns of time,\n" +
						"The Oppressor's wrong, the proud man's Contumely,\n" +
						"The pangs of dispised Love, the Law’s delay,\n" +
						"The insolence of Office, and the spurns\n" +
						"That patient merit of the unworthy takes,\n" +
						"When he himself might his Quietus make\n" +
						"With a bare Bodkin? Who would Fardels bear, [F: these Fardels]\n" +
						"To grunt and sweat under a weary life,\n" +
						"But that the dread of something after death,\n" +
						"The undiscovered country, from whose bourn\n" +
						"No traveller returns, puzzles the will,\n" +
						"And makes us rather bear those ills we have,\n" +
						"Than fly to others that we know not of.\n" +
						"Thus conscience does make cowards of us all,\n" +
						"And thus the native hue of Resolution\n" +
						"Is sicklied o'er, with the pale cast of Thought,\n" +
						"And enterprises of great pitch and moment, [F: pith]\n" +
						"With this regard their Currents turn awry, [F: away]\n" +
						"And lose the name of Action. Soft you now,\n" +
						"The fair Ophelia? Nymph, in thy Orisons\n" +
						"Be all my sins remember'd"
		);

		DataSet<Tuple2<String, Integer>> counts = text
				.flatMap(new SplitToWords())
				.groupBy(0)
				.sum(1);

		counts.print();

	}

	public static final class SplitToWords implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			Arrays
					.stream(value.toLowerCase().split("\\W+"))
					.filter(token -> token.length() > 0)
					.map(token -> new Tuple2<>(token, 1))
					.forEach(out::collect);
		}
	}
}
