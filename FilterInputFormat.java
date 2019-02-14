package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FilterInputFormat extends FileInputFormat<LongWritable, Text> {

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new InputReader();
	}

	/////////////////// ? MY RECORD READER//////////////////////
	static class InputReader extends RecordReader<LongWritable, Text> {
		private static final byte[] recordSeparator = "\n\n".getBytes();
		private FSDataInputStream fsin;
		private long start, end;
		private boolean stillInChunk = true;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) inputSplit;
			Configuration conf = context.getConfiguration();
			Path path = split.getPath();
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);

			fsin = fs.open(path);
			start = split.getStart();
			end = split.getStart() + split.getLength();
			fsin.seek(start);

			if (start != 0)
				readRecord(false);
		}

		private boolean readRecord(boolean withinBlock) throws IOException {
			int i = 0, b;
			while (true) {
				if ((b = fsin.read()) == -1)
					return false;
				if (withinBlock)
					buffer.write(b);
				if (b == recordSeparator[i]) {
					if (++i == recordSeparator.length)
						return fsin.getPos() < end;
				} else
					i = 0;
			}
		}

		public boolean nextKeyValue() throws IOException {
			if (!stillInChunk)
				return false;
			boolean status = readRecord(true);

			Text intermediateText = new Text();

			value = new Text();
			intermediateText.set(buffer.getData(), 0, buffer.getLength());
			String[] tokenizer = intermediateText.toString().split("\n");
			String[] revisionArray = tokenizer[0].split(" "); // REVISION 12 36302676 Anarchism 2006-01-23T02:39:36Z
																// RJII 141644
			value.set(revisionArray[4] + "\n" + revisionArray[3] + "\n" + tokenizer[3]); // Timestam, Article_title,  and
																							// Main + NEED TO INCLUDE
																							// PAGERANK FIELD FROM
																							// REDUCER OUTPUT FILE
			key.set(Long.valueOf(revisionArray[1])); // Article_id

			buffer.reset();
			if (!status)
				stillInChunk = false;
			return true;
		}

		public LongWritable getCurrentKey() {
			return key;
		}

		public Text getCurrentValue() {
			return value;
		}

		public float getProgress() throws IOException {
			return (float) (fsin.getPos() - start) / (end - start);
		}

		public void close() throws IOException {
			fsin.close();
		}

	}

}
