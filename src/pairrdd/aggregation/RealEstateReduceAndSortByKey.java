package pairrdd.aggregation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class RealEstateReduceAndSortByKey {

    public static void main(String args[]){
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("realEstate").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/RealEstate.csv");

        // quartos = 3 | preço = 2

        // gerando RDD com (quartos, preco + ocorrencia)
        JavaPairRDD<Integer, AvgCount> prdd = linhas.mapToPair(l -> {
            String[] valores = l.split(",");
            int quartos = 0;
            double preco = 0;
            if(!valores[3].equals("Bedrooms")) {
                quartos = Integer.parseInt(valores[3]);
            }
            if(!valores[2].equals("Price")) {
                preco = Double.parseDouble(valores[2]);
            }
            return new Tuple2<Integer, AvgCount>(quartos, new AvgCount(1, preco));
        });

        // somando precos e ocorrências por número de quartos
        JavaPairRDD<Integer, AvgCount> somas = prdd.reduceByKey((x, y) -> new AvgCount(x.getOcorrencia() + y.getOcorrencia(), x.getPreco() + y.getPreco()));

        // visualizando somas
        //somas.foreach(v -> System.out.println(v._1() + "|" + v._2()));

        // dividindo preço por ocorrência
        JavaPairRDD<Integer, Double> resultado = somas.mapValues(v -> v.getPreco() / v.getOcorrencia());

        // ordenar por número de quartos
        JavaPairRDD<Integer, Double> ordenado = resultado.sortByKey(true);

        // imprimindo resultados
        ordenado.collect().forEach(v -> System.out.println(v));

    }


}
