package temperature;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class maxreducer extends MapReduceBase implements Reducer
{ 
public void reduce(Text key, Iterator values, OutputCollector output, Reporter reporter) throws
IOException {
int max_temp = 0; //for minimum int max_temp = 100;
while (values.hasNext())
{
int current=values.next().get();
if ( max_temp < current) // for minimum if ( max_temp > current)
max_temp = current; }
output.collect(key, new IntWritable(max_temp/10)); } }