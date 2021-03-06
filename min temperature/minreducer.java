package temp;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class minreducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
IOException, InterruptedException {
int minValue = Integer.MAX_VALUE;
for (IntWritable value : values) {
minValue = Math.min(minValue, value.get());
}
context.write(key, new IntWritable(minValue));
}}