import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.Year;
import scala.Tuple2;
import org.jfree.chart.ChartFactory;
import java.awt.*;
import javax.swing.*;


import static java.lang.Integer.*;

public final class apache {

    public static int year(String date) {
        return parseInt(date.substring(date.length() - 4, date.length()));
    }

    public static void main(String args[]) {
        SparkSession sc = SparkSession.builder().appName("apache").config("spark.master", "local").getOrCreate();
        SQLContext sqlContext = new SQLContext(sc);
        // change path to dataset
        Dataset<Row> json_dataset = sqlContext.read().format("json").json("/Users/nikilreddy/Desktop/books_reviews.json");
        json_dataset.createOrReplaceTempView("plotting");
        Dataset<Row> json_dataset1 = sqlContext.sql("select asin,reviewTime from plotting");
        JavaRDD<Row> json_dataset2 = json_dataset1.javaRDD();

        JavaPairRDD<String, String> json_dataset3 = json_dataset2.mapToPair(row ->
                new Tuple2<>(row.getString(0), row.getString(1)));
        JavaPairRDD<String, Integer> json_dataset4 = json_dataset3.mapValues(apache::year);
        JavaPairRDD<Integer, Integer> json_dataset5 = json_dataset4.mapToPair(row ->
                new Tuple2<>(row._2(), 1)).reduceByKey((a, b) -> a + b);
        /*
        having problem creating an array from rdd, so just printing rdd values to console and manually assign it String array arr.
         */
        json_dataset5.foreach(data -> {
            System.out.println("year=" + data._1() + " count=" + data._2());
        });

        String[] arr = {"2013,2982481", "2014,1974115",
                "year=2012, count=1175910",
                "year=2011, count=593499",
                "year=2010, count=413181",
                "year=2009, count=349026",
                "year=2008, count=274605",
                "year=2007, count=237955",
                "year=2006, count=189278",
                "year=2005, count=163506",
                "year=2004, count=122834",
                "year=2003, count=104329",
                "year=2002, count=101028",
                "year=2001, count=97068",
                "year=2000, count=87855",
                "year=1999, count=19574",
                "year=1998, count=9731",
                "year=1997, count=2041",
                "year=1996, count=25"};


        final TimeSeries series = new TimeSeries("yearly analysis");
        for (String anArr : arr) {
            String[] temp = anArr.split(",");
            series.add(new Year(Integer.parseInt(temp[0].replaceAll("[^0-9]", ""))), new Integer(temp[1].replaceAll("[^0-9]", "")));
        }

        final JFreeChart chart = ChartFactory.createTimeSeriesChart("reviews per year analysis", "year", "reviews", new TimeSeriesCollection(series),false,false,false);
        chart.getPlot().setBackgroundPaint(Color.WHITE);
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(560,370));
        chartPanel.setMouseZoomable(true);
        JFrame frame = new JFrame();
        frame.setContentPane(chartPanel);
        frame.setVisible( true );
        }



    }

