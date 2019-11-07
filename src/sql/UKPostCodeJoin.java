package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UKPostCodeJoin {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("ukpostcode").master("local[2]").getOrCreate();

        // carregando arquivos
        Dataset<Row> postcode = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/uk-postcode.csv");

        Dataset<Row> makerspace = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/uk-makerspaces-identifiable-data.csv");

        // visualizando header das tabelas
        //postcode.show(5);
        //makerspace.show(5);

        // adicionar espaço no final da coluna postcode (postcode)
        postcode = postcode.withColumn("Postcode", // nome da nova coluna sobreescrita
                concat_ws("", col("Postcode"), lit(" ")));

        // mudando nome da coluna postcode
        postcode = postcode.withColumnRenamed("Postcode", "novoPostcode");

        // join makerspace + postcode c/ critério
        // coluna postcode (makerspace) começa com a coluna postcode (postcode)
        Dataset<Row> joined = makerspace.join(postcode,
                makerspace.col("Postcode").startsWith(postcode.col("novoPostcode")),
                "left_outer");

        // visualizando o resultado do join
        //joined.show(10);

        // selecionando apenas colunas do postcode
        joined.select("Postcode", "novoPostcode").show();

        // parando a sessão
        session.stop();

    }
}