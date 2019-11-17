package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;
// número de propostas de clientes que vivem próximos a uma região de risco e com renda maior que R$ 7.000
public class Exercicio7Treinamento {
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
        Dataset<Row> filtrado = arquivo.filter(col("DISTZONARISCO").$greater$eq(0));

        // buscando propostas com zona de risco menor que 5km
        Dataset<Row> zonaRisco = filtrado.filter(col("DISTZONARISCO").$less$eq(5000));

        // buscando renda maior que R$ 7.000
        Dataset<Row> renda = zonaRisco.filter(col("ESTIMATIVARENDA").$greater(7000));
        System.out.println(renda.count());

        // parando a sessão
        session.stop();
    }
}
