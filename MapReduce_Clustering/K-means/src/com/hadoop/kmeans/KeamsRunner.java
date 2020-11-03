package com.hadoop.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeamsRunner {
	 public static void run(String centerPath,String dataPath,String newCenterPath,boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException{
		 
	        Configuration conf = new Configuration();
	        conf.set("centersPath", centerPath);
	 
	        Job job = new Job(conf, "mykmeans");
	        job.setJarByClass(KeamsRunner.class);
	        job.setMapperClass(KeamsMapper.class);
	 
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(Text.class);
	 
	        if(runReduce){
	            //��������������Ҫreduce
	            job.setReducerClass(KeamsReduce.class);
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(Text.class);
	        }
	 
	        FileInputFormat.addInputPath(job, new Path(dataPath));
	 
	        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));
	 
	        System.out.println(job.waitForCompletion(true));
	    }
	 
	    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	        String centerPath = "hdfs://192.168.221.173:8020/input/centers.txt";//������ĵ�����ֵ
	        String dataPath = "hdfs://192.168.221.173:8020/input/test.csv";//��Ŵ���������
	        String newCenterPath = "hdfs://192.168.221.173:8020/out/kemans";//������Ŀ¼
	 
	        int count = 0;
	 
	 
	        while(true){
	            run(centerPath,dataPath,newCenterPath,true);
	            System.out.println(" �� " + ++count + " �μ��� ");
	            if(Utils.compareCenters(centerPath,newCenterPath )){
	                run(centerPath,dataPath,newCenterPath,false);
	                break;
	            }
	        }
	    }
}
