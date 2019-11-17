package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DataTreatment {
    public static void main(String[] args) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("omml").master("local[2]").getOrCreate();

        // carregando arquivos
        Dataset<Row> primeiro = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/ommlbd_basico.csv");

        Dataset<Row> segundo = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/ommlbd_empresarial.csv");

        Dataset<Row> terceiro = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/ommlbd_familiar.csv");

        Dataset<Row> quarto = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/ommlbd_regional.csv");

        Dataset<Row> quinto = session.read()
                .option("header", "true") // cabeçalho
                .option("inferSchema", "true") // inferindo tipos de dados
                .csv("in/ommlbd_renda.csv");

        // alterando colunas com mesmo nome (HS_CPF)
        Dataset<Row> empresarial = segundo.withColumnRenamed("HS_CPF", "HS_CPF_E");
        Dataset<Row> familiar = terceiro.withColumnRenamed("HS_CPF", "HS_CPF_F");
        Dataset<Row> regional = quarto.withColumnRenamed("HS_CPF", "HS_CPF_R");
        Dataset<Row> renda = quinto.withColumnRenamed("HS_CPF", "HS_CPF_RD");


        // join basico + empresarial
        Dataset<Row> firstJoin = primeiro
                .join(empresarial, primeiro.col("HS_CPF").equalTo(empresarial.col("HS_CPF_E")), "right_outer");

        // join fj + familiar
        Dataset<Row> secondJoin = firstJoin
                .join(familiar, firstJoin.col("HS_CPF").equalTo(familiar.col("HS_CPF_F")), "right_outer");

        // join sj + regional
        Dataset<Row> thirdJoin = secondJoin
                .join(regional, secondJoin.col("HS_CPF").equalTo(regional.col("HS_CPF_R")), "right_outer");

        // join tj + renda
        Dataset<Row> finalJoin = thirdJoin
                .join(renda, thirdJoin.col("HS_CPF").equalTo(renda.col("HS_CPF_RD")), "right_outer");

        // separando safras
        Dataset<Row> treino = finalJoin.filter(col("SAFRA").equalTo("TREINO"));
        Dataset<Row> teste = finalJoin.filter(col("SAFRA").equalTo("TESTE"));

        // verificando se as médias batem com as do dicionário de dados
        treino.select(avg(col("HS_CPF"))).show();
        teste.select(avg(col("HS_CPF"))).show();
        treino.select(avg("TEMPOCPF")).show();
        teste.select(avg("TEMPOCPF")).show();
        treino.select(avg("DISTCENTROCIDADE")).show();
        teste.select(avg("DISTCENTROCIDADE")).show();
        treino.select(avg("DISTZONARISCO")).show();
        teste.select(avg("DISTZONARISCO")).show();

        // salvando safras em arquivos .csv
        treino.coalesce(1).write()
                .mode("overwrite") // modo de escrita
                .format("csv") // tipo do arquivo de saída
                .option("header", "true") // com cabeçalho
                .save("in/dadostreino.csv"); // salva
        teste.coalesce(1).write()
                .mode("overwrite") // modo de escrita
                .format("csv") // tipo do arquivo de saída
                .option("header", "true") // com cabeçalho
                .save("in/dadosteste.csv"); // salva

        // parando a sessão
        session.stop();

        // TODO: verificar inconsistências para cada exercício
    }
}
