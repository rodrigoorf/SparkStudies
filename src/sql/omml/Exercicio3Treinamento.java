package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
// número de propostas cujo cliente possui estimativa de renda superior a R$ 10.000
public class Exercicio3Treinamento {
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

        // removendo resultados indesejáveis
        Dataset<Row> filtrado = arquivo.filter(col("ESTIMATIVARENDA").$greater$eq(0));

        // buscando ocorrências de propostas com renda maior que R$ 10.000
        long estimativarenda = filtrado.filter(col("ESTIMATIVARENDA").$greater(10000)).count();
        System.out.println(estimativarenda);

        // parando a sessão
        session.stop();
    }
}
