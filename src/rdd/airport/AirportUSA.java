package rdd.airport;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


public class AirportUSA {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]){
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // leitura do arquivo
        JavaRDD<String> linhas = sc.textFile("in/airports.text");

        // quebrando cada linha em palavras
        JavaRDD<String> palavras = linhas.filter(l -> {
            String[] split = l.split(COMMA_DELIMITER);
            if(split[3].equals("\"United States\"")){
                return true;
            } else {
                return false;
            }
        });

        List<String> lista = palavras.collect();
        for(String s : lista){
            System.out.println(s);
        }
    }
}
