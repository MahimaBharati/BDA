package empctc;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class employeedriver {
public static void main(String[] args) throws IOException,
InterruptedException, ClassNotFoundException {
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf,
args).getRemainingArgs();
if (otherArgs.length != 2) {
System.err.println("Usage: Employee CTC Anaysis
<input> <output>");
System.exit(2);
}
Job job = Job.getInstance(conf, "Employee CTC Anaysis");
job.setJobName("Custmom Patitioner");
job.setJarByClass(employeedriver.class);
job.setMapperClass(employeemapper.class);
job.setReducerClass(employeereducer.class);
job.setNumReduceTasks(2);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new
Path(otherArgs[0]));
FileOutputFormat.setOutputPath(job, new
Path(otherArgs[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}