package rdd.airport;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.columnar.DOUBLE;

public class AvgLatitude {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]){
        // Sets to ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("airportAvgLatitude").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // leitura do arquivo
        JavaRDD<String> linhas = sc.textFile("in/airports.text");

        // quebrando cada linha em palavras
        JavaRDD<String> palavras = linhas.filter(l -> {
            String[] split = l.split(COMMA_DELIMITER);
            // país: Estados Unidos | latitude maior que 40
            if(split[3].equals("\"United States\"") && Double.parseDouble(split[6]) > 40){
                return true;
            } else {
                return false;
            }
        });

        // recomendado deixar filtros separados (sem &&)

        // criando map latitudes
        JavaRDD<Double> latitudes = palavras.map(l -> Double.parseDouble(l.split(COMMA_DELIMITER)[6]));

        // calcular média latitudes
        Double soma = latitudes.reduce((x, y) -> x + y);
        long quantidade = latitudes.count();
        double media = soma / quantidade;

        System.out.println("Média: " + media);

        // salvando em arquivo de texto
        //palavras.saveAsTextFile("output/usaairports.text");




    }
}
