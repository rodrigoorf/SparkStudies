package pairrdd.filter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

public class FilterPairRDD {

    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main (String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("filterPairRDD").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando arquivo
        JavaRDD<String> linhas = sc.textFile("in/airports.text");

        // País = 3 | Cidade = 2

        // gerando PairRDD com país e cidade
        JavaPairRDD<String, String> prdd = linhas.mapToPair(pegaPaisECidade());

        // filtro para manter quem *não* está nos EUA
        JavaPairRDD<String, String> filtrado = prdd.filter(v -> !v._1().equals("\"United States\""));

        // salvar como arquivo
        //filtrado.coalesce(1).saveAsTextFile("output/aeroportos_fora_usa.text");

        // agrupando cidades por país
        JavaPairRDD<String, Iterable<String>> agrupado = filtrado.groupByKey();

        agrupado.foreach(v -> System.out.println(v._1() + " | " + v._2()));

    }

    private static PairFunction<String, String, String> pegaPaisECidade(){
        return line -> {
            String[] valores = line.split(COMMA_DELIMITER);
            String pais = valores[3];
            String cidade = valores[2];
            return new Tuple2<String, String>(pais, cidade);
        };
    }
}
