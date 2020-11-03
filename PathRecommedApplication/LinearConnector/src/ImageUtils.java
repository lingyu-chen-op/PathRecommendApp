import org.opencv.core.Mat;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;

public class ImageUtils {
    public static final int BLACK = 0;
    public static final int WHITE = 255;

    private Mat mat;

    public ImageUtils() {

    }

    @Override
    public String toString() {
        return "ImageUtils{" +
                "mat=" + mat +
                '}';
    }

    public Mat getMat(){
        return this.mat;
    }

    public void setMat(Mat m){
        mat = m;
    }

    public ImageUtils(String imgFilePath) {
        mat = Imgcodecs.imread(imgFilePath);
    }

    public void loadImg(String path) {
        this.mat = Imgcodecs.imread(path);
    }

    public void writeImg(String path) {
        Imgcodecs.imwrite(path, mat);
    }

    public int getHeight() {
        return this.mat.height();
    }

    public int getWidth() {
        return this.mat.width();
    }

    public void setPixel(int row, int col, double[] color) {
        this.mat.put(row, col, color);
    }

    public int getPixel(int row, int col) {
        return (int) this.mat.get(row, col)[0];
    }

    public double[] getColor(int row, int col) {
        double[] d = mat.get(row, col);
        return d;
    }
}
