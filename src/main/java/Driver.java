import java.io.*;
import java.lang.Object;
import java.util.*;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.PairFunction;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ie.util.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.*;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.*;
import edu.stanford.nlp.trees.TreeCoreAnnotations;

import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.stream.Collectors;


public final class Driver {

    private static PairFunction<Tuple2<String, Tuple2<Double, Integer>>,String,Double> getAverageByKey = (tuple) -> {
        Tuple2<Double, Integer> val = tuple._2;
        double total = val._1;
        double count = val._2;
        Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, Math.round((total / count)*100.0)/100.0);
        return averagePair;
    };

    private static Double sqr(Tuple2<Double, Double> v){
        return Math.pow(Math.abs(v._1-v._2),2);
    }

    private static Double mae(Tuple2<Double, Double> v){
        return Math.abs(v._1-v._2);
    }
    
    public static void main(String[] args) throws Exception {

        SparkSession sc = SparkSession
                .builder()
                .appName("Driver")
                .getOrCreate();

        //Local Application initialization
        //SparkConf conf = new SparkConf().setMaster("spark://jackson:30280").setAppName("IdealPageRank");
        //JavaSparkContext sc = new JavaSparkContext(conf);

        //read in the reviews from input json file and put into the Dataset
        Dataset<Row> json_dataset = sc.read().json(args[0]);

	//put the asin and user given star ratings into JavaRDD
        JavaPairRDD<Double, String> overall_review = json_dataset.javaRDD().mapToPair( row -> new Tuple2<>(row.getDouble(2), row.getString(3)));

	JavaPairRDD<Double, Double> nlpRatingsCompare;

	//returns an RDD with <overall, nlp calculated rating>
        switch (args[1]) {
            case "np": 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.noPriority(review) );
                break;
            case "fso": 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.firstSentenceOnly(review) );
                break;
            case "fals": 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.firstAndLastSentence(review) );
                break;
            case "mo": 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.middleOnly(review) );
                break;
            case "sflam": 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.splitFirstLastAndMiddle(review) );
                break;
            case "ms": 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.maxSentence(review) );
                break;
            case "mss": 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.maxSentenceSplit(review) );
                break;
            default: 
                nlpRatingsCompare = overall_review.mapValues( review -> Algorithms.noPriority(review) );
        }
        
	nlpRatingsCompare.saveAsTextFile(args[1] + "_overall_output");

	//put the asin and user given star ratings into JavaRDD
        JavaPairRDD<String, Double> asin_overall = json_dataset.javaRDD().mapToPair( row -> 
            new Tuple2<>(row.getString(0), row.getDouble(2)));
            
        //put the user give and user written review into JavaRDD
        JavaPairRDD<String, String> asin_review = json_dataset.javaRDD().mapToPair( row -> 
            new Tuple2<>(row.getString(0), row.getString(3)));

        JavaPairRDD<String, Double> nlpRatings;
        
        //returns an RDD with <asin, nlp calculated rating>
        switch (args[1]) {
            case "np": 
                nlpRatings = asin_review.mapValues( review -> Algorithms.noPriority(review) );
                break;
            case "fso": 
                nlpRatings = asin_review.mapValues( review -> Algorithms.firstSentenceOnly(review) );
                break;
            case "fals": 
                nlpRatings = asin_review.mapValues( review -> Algorithms.firstAndLastSentence(review) );
                break;
            case "mo": 
                nlpRatings = asin_review.mapValues( review -> Algorithms.middleOnly(review) );
                break;
            case "sflam": 
                nlpRatings = asin_review.mapValues( review -> Algorithms.splitFirstLastAndMiddle(review) );
                break;
            case "ms": 
                nlpRatings = asin_review.mapValues( review -> Algorithms.maxSentence(review) );
                break;
            case "mss": 
                nlpRatings = asin_review.mapValues( review -> Algorithms.maxSentenceSplit(review) );
                break;
            default: 
                nlpRatings = asin_review.mapValues( review -> Algorithms.noPriority(review) );
        }       

        //Calculate averages for each of the asin for the overall ratings (from JSON)
        //count each values per key
        JavaPairRDD<String, Tuple2<Double, Integer>> valueCount = asin_overall.mapValues(value -> new Tuple2<>(value,1));
        //add values by reduceByKey
        JavaPairRDD<String, Tuple2<Double, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        //calculate average
        JavaPairRDD<String, Double> overallAveragePair = reducedCount.mapToPair(getAverageByKey);

        //Calculate averages for each of the asin for the overall ratings (from JSON)
        //count each values per key
        JavaPairRDD<String, Tuple2<Double, Integer>> valueCount2 = nlpRatings.mapValues(value -> new Tuple2<>(value,1));
        //add values by reduceByKey
        JavaPairRDD<String, Tuple2<Double, Integer>> reducedCount2 = valueCount2.reduceByKey((tuple1,tuple2) ->  new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        //calculate average
        JavaPairRDD<String, Double> nlpRatingsAveragePair = reducedCount2.mapToPair(getAverageByKey);

        //join together the two averages (one using overall and one using nlpRankings) by product ID
        JavaPairRDD<String, Tuple2<Double, Double>> joinedAverages = overallAveragePair.join(nlpRatingsAveragePair);
        
        //output file (asin, [avergae overallRating, avergae nlpRanking])
        joinedAverages.saveAsTextFile(args[1] + "_average_output");
        
        JavaPairRDD<String, Double> RMSE = joinedAverages.mapValues(value -> sqr(value));
	JavaPairRDD<String, Double> MAE = joinedAverages.mapValues(value -> mae(value));
        long count = RMSE.count();
	double RMSESum = RMSE.values().reduce((a,b) -> a+b);
	double MAESum = MAE.values().reduce((a,b) -> a+b);

	Double RMSEError = Math.sqrt(RMSESum / count);
	Double MAEError = MAESum / count;
	
        RMSE.saveAsTextFile(args[1] + "_RMSE_output_" + RMSEError);
	MAE.saveAsTextFile(args[1] + "_MAE_output_" + MAEError);
        
        sc.stop();
    }
}
