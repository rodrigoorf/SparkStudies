package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

// número máximo e mínimo de e-mails cadastrados em toda a base de dados
public class Exercicio2Teste {
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

        // filtrando resultados indesejáveis (-9999)
        Dataset<Row> filtrado = arquivo.filter(col("QTDEMAIL").$greater$eq(0));

        // buscando máximo e mínimo de e-mails
        filtrado.select(max("QTDEMAIL"), min("QTDEMAIL")).show();

        // parando a sessão
        session.stop();
    }
}
