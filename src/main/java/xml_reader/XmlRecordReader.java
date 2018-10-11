package xml_reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class XmlRecordReader extends RecordReader<LongWritable, Text> {
    private final byte[][] tagNames;
    private final byte[][] tagNamesEnd;
    private long start;
    private long end;
    private FSDataInputStream inputStream;

    public XmlRecordReader(String... tagNames) {
        this.tagNames = new byte[tagNames.length][];
        this.tagNamesEnd = new byte[tagNames.length][];
        for (int i = 0; i < tagNames.length; i++) {
            try {
                this.tagNames[i] = ("<" + tagNames[i] + " ").getBytes("UTF-8");
                this.tagNamesEnd[i] = ("/>").getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit filesplit = (FileSplit) split;
        start = filesplit.getStart();
        end = start + filesplit.getLength();
        FileSystem fs = filesplit.getPath().getFileSystem(context.getConfiguration());
        inputStream = fs.open(filesplit.getPath());
        inputStream.seek(start);
    }


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        System.out.println("currentValue:"+currentValue);
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        long total = end - start;
        long read = inputStream.getPos() - start;
        return (read / (float) total);
    }

    DataOutputBuffer buf = new DataOutputBuffer();
    LongWritable currentKey;
    Text currentValue;

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (inputStream.getPos() >= end) {
            return false;
        }

        if (readUntilMatch(tagNames, false)) {
            try {
                buf.write(currentlyMatchedTag);
                System.out.println("buf:"+buf.toString());
                if (readUntilMatch(tagNamesEnd, true)) {
                    currentKey = new LongWritable(inputStream.getPos());
                    currentValue = new Text();
                    currentValue.set(buf.getData(), 0, buf.getLength());
                    return true;
                }
            } finally {
                buf.reset();
            }
        }
        return false;
    }

    private byte[] currentlyMatchedTag;

    private boolean readUntilMatch(byte[][] matches, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int byt = inputStream.read();
            if (byt == -1) {
                return false;
            }
            if (withinBlock) {
                buf.write(byt);
            }
            boolean matching = false;
            for (byte[] match : matches) {
                if (i < match.length && byt == match[i]) {
                    i++;
                    if (i >= match.length) {
                        currentlyMatchedTag = match;
                        return true;
                    }
                    matching = true;
                    break;
                }
            }

            if (!matching) {
                i = 0;
            }

            if (!withinBlock && i == 0 && inputStream.getPos() >= end) {
                return false;
            }
        }
    }


    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
