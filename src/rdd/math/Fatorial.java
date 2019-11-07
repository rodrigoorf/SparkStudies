package rdd.math;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;

public class Fatorial {

    public static void main (String args[]){

        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("factorial").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // n! n * (n-1) * (n-2) * ... * 1

        long n = 5;
        // armazenando os valores de 1 até n na lista
        LinkedList<Long> valores = new LinkedList<>();
        while(n != 1){
            valores.add(n);
            n--;
        }

        // reduzindo o RDD usando multiplicações
        JavaRDD<Long> rdd = sc.parallelize(valores);
        Long fatorial = rdd.reduce((x, y) -> x * y);

        System.out.println("Fatorial: " + fatorial);
    }
}
