package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

// número de proposta de clientes adimplentes e inadimplentes com renda maior que R$ 5.000 e sócios de empresa ou não
public class Exercicio8Teste {
    public static void main(String[] args) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("omml").master("local[2]").getOrCreate();

        // carregando arquivo
        Dataset<Row> arquivo = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/dadosteste.csv");

        // obtendo apenas propostas onde o cliente possui renda maior que R$ 5.000
        Dataset<Row> renda = arquivo.filter(col("ESTIMATIVARENDA").$greater(5000));

        // mostrando resultados com base em adimplência e flag se é sócio
        renda.groupBy("TARGET", "SOCIOEMPRESA").count().show();

        // parando a sessão
        session.stop();
    }
}
