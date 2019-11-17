package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
// percentual de propostas de crédito cujo cliente possui um funcionário público em casa
public class Exercicio5Treinamento {
    public static void main(String[] args) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("omml").master("local[2]").getOrCreate();

        // carregando arquivo
        Dataset<Row> arquivo = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/dadostreino.csv");

        // buscando apenas resultados com clientes que convivem com um funcionário público em casa
        Dataset<Row> filtrado = arquivo.filter(col("FUNCIONARIOPUBLICOCASA").equalTo(1));

        // cálculo do percentual
        double select = filtrado.count();
        double all = arquivo.count();
        double percentual = (select / all) * 100;
        System.out.println(percentual + "%");

        // parando a sessão
        session.stop();
    }
}
