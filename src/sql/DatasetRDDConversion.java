package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class DatasetRDDConversion {

    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // carregando os dados em um RDD tradicional
        JavaRDD<String> rdd = sc.textFile("in/2016-stack-overflow-survey-responses.csv");

        // filtrando cabeçalho
        JavaRDD<String> rddFiltrado = rdd.filter(v -> !v.startsWith(",collector,"));

        // mapeando linhas em objetos do tipo Response
        JavaRDD<Response> rddResponse = rddFiltrado.map(v -> {
            String[] valores = v.split(COMMA_DELIMITER, -1);
            String occupation = valores[9];
            double age = valores[6].isEmpty() ? 0.0 : Double.parseDouble(valores[6]);
            double salary = valores[14].isEmpty() ? 0.0 : Double.parseDouble(valores[14]);
            return new Response(occupation, age, salary);
        });

        // verificando valores
        // rddResponse.collect().forEach(v -> System.out.println(v.toString()));

        // convertendo de RDD para Dataset
        Dataset<Response> dataset = session.createDataset(rddResponse.rdd(), Encoders.bean(Response.class));

        // verificar a conversão
        dataset.show();

        // conversão de Dataset para RDD
        JavaRDD<Response> retornado = dataset.toJavaRDD();

        // parando a sessão
        session.stop();


    }
}
