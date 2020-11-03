package com.hadoop.kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

	public class KeamsMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		 
        //中心集合
        ArrayList<ArrayList<Double>> centers = null;
        //用k个中心
        int k = 0;
 
        //读取中心
        protected void setup(Context context) throws IOException,
                InterruptedException {
            centers = Utils.getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
            k = centers.size();//获取中心点个数
        }
 
 
        /**
         * 1.每次读取一条要分类的条记录与中心做对比，归类到对应的中心
         * 2.以中心ID为key，中心包含的记录为value输出(例如： 1 0.2---->1为聚类中心的ID，0.2为靠近聚类中心的某个值)
         */
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //读取一行数据
            ArrayList<Double> fileds = Utils.textToArray(value);
            int sizeOfFileds = fileds.size();
 
            double minDistance = 99999999;
            int centerIndex = 0;
 
            //依次取出k个中心点与当前读取的记录做计算
            for(int i=0;i<k;i++){
                double currentDistance = 0;
                for(int j=1;j<sizeOfFileds;j++){
                    double centerPoint = Math.abs(centers.get(i).get(j));
                    double filed = Math.abs(fileds.get(j));
                    currentDistance += Math.pow((centerPoint - filed) / (centerPoint + filed), 2);//这里的距离算法简化处理
                }
                //循环找出距离该记录最接近的中心点的ID
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
            //以中心点为Key 将记录原样输出
            context.write(new IntWritable(centerIndex+1), value);
        }
}
