# CSYE7200_Project 
*[Demo](https://github.com/desperadomzc/CSYE7200_Project/blob/master/Project%20Demo%20-%20Path%20Recommend.pptx)*

Scala Final Project--Path Recommend System

The idea of our system originated from the game Line Connector, which requires players to find the only path that traverse all nodes of the picture with given start point and without repetition. Respectively, we will construct a system to provide path recommendation for users to visit specific spots they plan to visit with a shortest path to visit all the spots without repetition.

We use Depth First Search algorithm to solve the game problem. However, when we try to migrate our idea to the real-world big data problem, we need to improve some part of the algorithm:

  •	For that DFS is some kind of violent enumeration, we need to reduce our possible traverse cases for the big data problem
  
  •	Appropriately reduce the problem dimension, for example, we extract effective subgraph for the large-scale network

And also, we use the Spark GraphX to do the distributed graph calculation.


The solution for the game part is:

  •	Convert the picture to pixel matrix for system to operate by Image Parsing based on OpenCV
  
  •	Convert the pixel matrix to sparse matrix and then write into .txt file 

  •	Utilize Spark GraphX GraphLoader to read the .txt file
  
  •	Out put the solution

The solution for the road network path recommendation is:

  •	Utilize Spark GraphX GraphLoader to read the .txt file of Pennsylvania road network
  
  •	Extract subgraph based on the spots that the user plan to visit
  
  •	Get the shortest paths from all the nodes in subgraph to the given start point
  
  •	Filter the shortest paths and then find out desired recommendation



Project Structure:

HelloOpenCV(LineConnector): the module to convert game screen shoots to matrixes and .txt files
PathRecommendation: the module to do the graph calculation and encapsulations of GraphUtils and DFS methods

If you need to run the project:
Feel free to change the path for .txt files to your prefered relative paths. And be careful that HelloOpenCV based on OpenCV for Java, and PathRecommendation based on Scala 2.11 and Spark 2.4
