package com.hadoop.kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

	public class KeamsMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		 
        //���ļ���
        ArrayList<ArrayList<Double>> centers = null;
        //��k������
        int k = 0;
 
        //��ȡ����
        protected void setup(Context context) throws IOException,
                InterruptedException {
            centers = Utils.getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
            k = centers.size();//��ȡ���ĵ����
        }
 
 
        /**
         * 1.ÿ�ζ�ȡһ��Ҫ���������¼���������Աȣ����ൽ��Ӧ������
         * 2.������IDΪkey�����İ����ļ�¼Ϊvalue���(���磺 1 0.2---->1Ϊ�������ĵ�ID��0.2Ϊ�����������ĵ�ĳ��ֵ)
         */
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //��ȡһ������
            ArrayList<Double> fileds = Utils.textToArray(value);
            int sizeOfFileds = fileds.size();
 
            double minDistance = 99999999;
            int centerIndex = 0;
 
            //����ȡ��k�����ĵ��뵱ǰ��ȡ�ļ�¼������
            for(int i=0;i<k;i++){
                double currentDistance = 0;
                for(int j=1;j<sizeOfFileds;j++){
                    double centerPoint = Math.abs(centers.get(i).get(j));
                    double filed = Math.abs(fileds.get(j));
                    currentDistance += Math.pow((centerPoint - filed) / (centerPoint + filed), 2);//����ľ����㷨�򻯴���
                }
                //ѭ���ҳ�����ü�¼��ӽ������ĵ��ID
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
            //�����ĵ�ΪKey ����¼ԭ�����
            context.write(new IntWritable(centerIndex+1), value);
        }
}
