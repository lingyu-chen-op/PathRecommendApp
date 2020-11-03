import java.io.*;
import java.util.Arrays;

public class Matrix {
    private int rowNum;
    private int colNum;
    private int[][] matrix;
    private int startX;
    private int startY;

    public int getStartX() {
        return startX;
    }

    public void setStartX(int startX) {
        this.startX = startX;
    }

    public int getStartY() {
        return startY;
    }

    public void setStartY(int startY) {
        this.startY = startY;
    }

    public Matrix(int rowNum, int colNum) {
//        if (rowNum <= 0 || colNum <= 0)
//            throw new Exception("ZeroColOrRowNumberException");
        this.rowNum = rowNum;
        this.colNum = colNum;
        this.matrix = new int[rowNum][colNum];
    }

    public void writeIntoEdgeListTxt(String edgepath, String startpath) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowNum - 1; i++) {
            for (int j = 0; j < colNum - 1; j++) {
                if (matrix[i][j] != 0) {
                    if (matrix[i + 1][j] != 0) {
                        String s1 = (i + 1) + "" + (j + 1) + " " + ((i + 2) + "" + (j + 1)) + "\n" + (i + 2) + "" + (j + 1) + " " + ((i + 1) + "" + (j + 1)) + "\n";
                        sb.append(s1);
                    }
                    if (matrix[i][j + 1] != 0) {
                        String s2 = (i + 1) + "" + (j + 1) + " " + ((i + 1) + "" + (j + 2)) + "\n" + (i + 1) + "" + (j + 2) + " " + ((i + 1) + "" + (j + 1)) + "\n";
                        sb.append(s2);
                    }
                }
            }
        }
        for (int m = 0; m < rowNum - 1; m++) {
            if (matrix[m][colNum - 1] != 0 && matrix[m + 1][colNum - 1] != 0) {
                String s1 = ((m + 1) + "" + colNum) + " " + ((m + 2) + "" + colNum) + "\n" + ((m + 2) + "" + colNum) + " " + ((m + 1) + "" + colNum) + "\n";
                sb.append(s1);
            }
        }
        for (int n = 0; n < colNum - 1; n++) {
            if (matrix[rowNum - 1][n] != 0 && matrix[rowNum - 1][n + 1] != 0) {
                String s1 = (rowNum + "" + (n + 1)) + " " + (rowNum + "" + (n + 2)) + "\n" + (rowNum + "" + (n + 2)) + " " + (rowNum + "" + (n + 1)) + "\n";
                sb.append(s1);
            }
        }
        try {
            File edgefile = new File(edgepath);
            PrintStream psedge = new PrintStream(new FileOutputStream(edgefile));
            psedge.print("#Name:edgeList " + "\n" + "#@author: Miao,Linh" + "\n");
            psedge.append(sb);
            File startfile = new File(startpath);
            PrintStream psstart = new PrintStream(new FileOutputStream(startfile));
            String xy = (startX+1) + "" + (startY+1);
            psstart.append(xy);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void setNode(int x, int y, int val) {
//        if (x >= this.rowNum || x < 0)
//            throw new Exception("RowNumberOutOfRangeException:" + "[" + 0 + "," + rowNum + "-1]");
//        if (y >= this.rowNum || y < 0)
//            throw new Exception("RowNumberOutOfRangeException:" + "[" + 0 + "," + colNum + "-1]");
//        if (val == 2)
        this.matrix[x][y] = val;
    }

    public int getNode(int x, int y) {
//        if (x >= this.rowNum || x < 0)
//            throw new Exception("RowNumberOutOfRangeException:" + "[" + 0 + "," + rowNum + "-1]");
//        if (y >= this.rowNum || y < 0)
//            throw new Exception("RowNumberOutOfRangeException:" + "[" + 0 + "," + colNum + "-1]");
        return this.matrix[x][y];
    }

    @Override
    public String toString() {
        String s = "Matrix = {\n";
        for (int i = 0; i < rowNum; i++) {
            s += "[";
            for (int j = 0; j < colNum; j++) {
                if (j != 0) s += " , " + matrix[i][j];
                else s += matrix[i][j];
            }
            s += "]" + "\n";
        }
        s += "}";
        return s;
    }

    public int getRowNum() {
        return rowNum;
    }

    public int getColNum() {
        return colNum;
    }

    public int size() {
        return rowNum * colNum;
    }


}
