import java.util.*;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.*;


public class Algorithms {

    private static ArrayList<Integer> sentimentScores(String review) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,parse,sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        Annotation document = new Annotation(review);
        pipeline.annotate(document);

        ArrayList<Integer> sentence_sentiment_scores = new ArrayList<Integer>();
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);

            switch (sentiment) {
                case "Strongly Positive":
                    sentence_sentiment_scores.add(5);
                    break;
                case "Positive":
                    sentence_sentiment_scores.add(4);
                    break;
                case "Neutral":
                    sentence_sentiment_scores.add(3);
                    break;
                case "Negative":
                    sentence_sentiment_scores.add(2);
                    break;
                default: // "Strongly Negative":
                    sentence_sentiment_scores.add(1);
            }
        }

        return sentence_sentiment_scores;
    }

    public static double noPriority(String review) {
        ArrayList<Integer> sentenceSentimentScores = sentimentScores(review);
        double sum = 0.0;

        for (int score : sentenceSentimentScores)
            sum += score;

        return sum / (sentenceSentimentScores.size());
    }

    public static double firstSentenceOnly(String review) {
        ArrayList<Integer> sentenceSentimentScores = sentimentScores(review);
        return sentenceSentimentScores.get(0);
    }

    public static double firstAndLastSentence(String review) {
        ArrayList<Integer> sentenceSentimentScores = sentimentScores(review);
        return (sentenceSentimentScores.get(0) + sentenceSentimentScores.get(sentenceSentimentScores.size() - 1)) / 2.0;
    }

    public static double middleOnly(String review) {
        ArrayList<Integer> sentenceSentimentScores = sentimentScores(review);
        double sum = 0.0;

        for (int score = 1; score < sentenceSentimentScores.size() - 1; score++)
            sum += sentenceSentimentScores.get(score);

        return sum / (sentenceSentimentScores.size() - 2);
    }

    public static double splitFirstLastAndMiddle(String review) {
        ArrayList<Integer> sentenceSentimentScores = sentimentScores(review);

        int first = sentenceSentimentScores.get(0);
        int last = sentenceSentimentScores.get(sentenceSentimentScores.size() - 1);
        double middleSum = 0.0;

        for (int score = 1; score < sentenceSentimentScores.size() - 1; score++)
            middleSum += sentenceSentimentScores.get(score);

        double rating = ((0.65 * (first + last)) + (0.35 * middleSum)) / sentenceSentimentScores.size();

        return (Math.round(rating * 100.0) / 100.0);
    }

    public static double maxSentence(String review) {
        ArrayList<Integer> sentenceSentimentScores = sentimentScores(review);
        int maxIndex = sentenceSentimentScores.indexOf(Collections.max(sentenceSentimentScores));
        return sentenceSentimentScores.get(maxIndex);
    }

    public static double maxSentenceSplit(String review) {
        ArrayList<Integer> sentenceSentimentScores = sentimentScores(review);
        int maxIndex = sentenceSentimentScores.indexOf(Collections.max(sentenceSentimentScores));
        double maxValue = sentenceSentimentScores.get(maxIndex);

        double sum = 0;

        for (int score = 0; score < sentenceSentimentScores.size() - 1; score++)
            if (score != maxIndex)
                sum += sentenceSentimentScores.get(score);

        double rating = (0.8 * maxValue + 0.2 * sum / sentenceSentimentScores.size());
        return rating;
    }
    
}
