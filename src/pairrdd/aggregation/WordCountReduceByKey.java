package pairrdd.aggregation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCountReduceByKey {

    public static void main(String args[]){
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/word_count.text");

        // quebrando linhas em palavras
        JavaRDD<String> palavras = linhas.flatMap(linha -> Arrays.asList(linha.split(" ")).iterator());

        // para cada palavra p, gerando (p, ocorrencia = 1)
        JavaPairRDD<String, Integer> ocorrencias = palavras.mapToPair(p -> new Tuple2<String, Integer>(p, 1));

        // reduceByKey() para contar as ocorrencias
        JavaPairRDD<String, Integer> contagem = ocorrencias.reduceByKey((x, y) -> x + y);

        //contagem.collect().forEach(v -> System.out.println(v));
        //contagem.foreach(v -> System.out.println(v));

        // revertendo o RDD
        JavaPairRDD<Integer, String> revert = contagem.mapToPair(p -> new Tuple2<>(p._2(), p._1()));

        // realizando o sortByKey()
        JavaPairRDD<Integer, String> sbk = revert.sortByKey(false);

        // re-revertendo
        JavaPairRDD<String, Integer> recontagem = sbk.mapToPair(p -> new Tuple2<>(p._2(), p._1()));
        recontagem.collect().forEach(v -> System.out.println(v));



    }
}
