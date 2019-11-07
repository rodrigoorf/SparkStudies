package rdd.nasa;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Union {
    public static void main(String args[]){
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Carregando os RDDs
        JavaRDD<String> rddA = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> rddB = sc.textFile("in/nasa_19950801.tsv");

        // concatenando os RDDs
        JavaRDD<String> uniao = rddA.union(rddB);

        // removendo cabeçalhos
        JavaRDD<String> filtrado = uniao.filter(v -> !v.startsWith("host") && !v.endsWith("bytes"));

        //filtrado.saveAsTextFile("output/nasa_parte_1.tsv");

        // obtendo apenas coluna de bytes e armazenando 10% delas em disco

        JavaRDD<Double> bytes = filtrado.map(l -> Double.parseDouble(l.split("\t")[5]));
        JavaRDD<Double> amostra = bytes.sample(false, 0.1);
        amostra.saveAsTextFile("output/nasa_parte_2.tsv");

    }

}
