package com.hadoop.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

	 public class KeamsReduce extends Reducer<IntWritable, Text, Text, Text>{
		 
	        /**
	         * 1.KeyΪ�������ĵ�ID valueΪ�����ĵļ�¼����
	         * 2.�������м�¼Ԫ�ص�ƽ��ֵ������µ�����
	         */
	        protected void reduce(IntWritable key, Iterable<Text> value,Context context)
	                throws IOException, InterruptedException {
	            ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();
	 
	            //���ζ�ȡ��¼����ÿ��Ϊһ��ArrayList<Double>
	            for(Iterator<Text> it =value.iterator();it.hasNext();){
	                ArrayList<Double> tempList = Utils.textToArray(it.next());
	                filedsList.add(tempList);
	            }
	 
	            //�����µ�����
	            //ÿ�е�Ԫ�ظ���
	            int filedSize = filedsList.get(0).size();
	            double[] avg = new double[filedSize];
	            for(int i=1;i<filedSize;i++){
	                //��ÿ�е�ƽ��ֵ
	                double sum = 0;
	                int size = filedsList.size();
	                for(int j=0;j<size;j++){
	                    sum += filedsList.get(j).get(i);
	                }
	                avg[i] = sum / size;
	            }
	            context.write(new Text("") , new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
	        }
	 }
