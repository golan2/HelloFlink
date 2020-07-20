package golan.hello.flink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

import java.io.Serializable;


/**
 * SIMPLE Streaming Job
 * Generate random numbers.
 * Write to file.
 */
public class StreamingJob {

	private static final String OUTPUT_FILE = "/tmp/StreamingJob";

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		final StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path(OUTPUT_FILE), new SimpleStringEncoder<String>("UTF-8"))
				.build();

		executionEnvironment
				.disableOperatorChaining()
				.addSource(new FromIteratorFunction<>(new StringSeqIterator()), TypeInformation.of(String.class))
				.name("StringSeqIterator")
				.uid("StringSeqIterator")
				.setParallelism(1)
				.rebalance()
				.addSink(sink)
				.setParallelism(1)
				.name("writeToFile [/tmp/StreamingJob]");


		executionEnvironment.execute("Flink StreamingJob");
	}


	private static class StringSeqIterator implements java.util.Iterator<String>, Serializable {
		private int val;

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public String next() {
			try {
				Thread.sleep(500);
				return "Message_" + val++;
			} catch (InterruptedException e) {
				throw new RuntimeException("Failed to generate next value", e);
			}
		}

		@Override
		public void remove() {
			throw new IllegalStateException("Unsupported operation");
		}
	}
}
