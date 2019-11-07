package rdd.math;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HelloWorld {

    public static void main(String args[]){
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de todas as threads possíveis (*)
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");
        // para rodar com 2 threads (exemplo)
        //SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // leitura do arquivo
        JavaRDD<String> linhas = sc.textFile("in/word_count.text");

        // quebrando cada linha em palavras
        JavaRDD<String> palavras = linhas.flatMap(l -> Arrays.asList(l.split(" ")).iterator());

        // contagem de palavras
        Map<String, Long> ocorrencias = palavras.countByValue();

        // apresentar resultados
        for (String p : ocorrencias.keySet()) {
            System.out.println(p + ": " + ocorrencias.get(p));
        }

    }
}
