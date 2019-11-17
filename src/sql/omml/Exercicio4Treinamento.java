package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
// número de propostas de crédito cujo cliente é beneficiário do bolsa família
public class Exercicio4Treinamento {
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

        // buscando apenas resultados com clientes beneficiários do bolsa família
        Dataset<Row> filtrado = arquivo.filter(col("BOLSAFAMILIA").equalTo(1));

        // imprimindo quantidade de clientes beneficiários
        System.out.println(filtrado.count());

        // parando a sessão
        session.stop();
    }
}
