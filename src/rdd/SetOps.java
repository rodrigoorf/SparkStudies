package rdd;

import javafx.beans.binding.IntegerBinding;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import sun.awt.windows.WPrinterJob;

import java.util.Arrays;
import java.util.List;

public class SetOps {
    public static void main (String args[]){
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("setOps").setMaster("local[2]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // criando 2 RDDs
        JavaRDD<Integer> rddA = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8));
        JavaRDD<Integer> rddB = sc.parallelize(Arrays.asList(1,2,3,10,15,20));

        // união
        JavaRDD<Integer> uniao = rddA.union(rddB);
        // transforma em lista e itera para imprimir resultados
        //uniao.collect().forEach(v -> System.out.println(v));

        // subtração
        JavaRDD<Integer> subtracao = rddA.subtract(rddB);
        //subtracao.collect().forEach(v -> System.out.println(v));

        // interseção
        JavaRDD<Integer> intersecao = rddA.intersection(rddB);
        //intersecao.collect().forEach(v -> System.out.println(v));

        // produto cartesiano
        JavaPairRDD<Integer, Integer> cartesiano = rddA.cartesian(rddB);
        //cartesiano.collect().forEach(v -> System.out.println(v._1 + " | " + v._2));

        // distinct
        JavaRDD<Integer> distintos = uniao.distinct();
        //distintos.collect().forEach(v -> System.out.println(v));

        // take
        List<Integer> take = uniao.take(2);
        take.forEach(v -> System.out.println(v));
    }
}
