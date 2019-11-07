package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class StackOverFlowSQL {

    public static void main(String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("stackoverflow").master("local[2]").getOrCreate();

        // lendo os dados do arquivo
        Dataset<Row> dados = session.read().option("header", "true").option("inferSchema", "true")
                .csv("in/2016-stack-overflow-survey-responses.csv");

        // olhando o schema
        dados.printSchema();

        // verificando as 10 primeiras linhas
        //dados.show(10);

        // convertendo o tipo da coluna (age_midpoint)
//        dados = dados.withColumn("age_midpoint", // nome da nova coluna
//                col("age_midpoint") // nome da coluna antiga
//                        .cast("double"));

        // olhando o novo schema
//        dados.printSchema();

        // Select (obtendo colunas específicas)
        dados.select(col("country"), col("star_wars_vs_star_trek")).show();

        // Where -> Filter
        dados.filter(col("country").equalTo("Brazil")).show();

        // obtendo idade < 20
        dados.filter(col("age_midpoint").$less(20)).show();

        // ordenando  (salário)
        dados.sort(col("salary_midpoint").desc()).show();

        // agrupando dados -> salário médio por país
        Dataset<Row> salvar = dados.groupBy(col("country")) // agrupa por país
                .agg(avg(col("salary_midpoint"))) // média de salário
                .sort(col("avg(salary_midpoint)").desc());//.show() // ordena

        salvar.coalesce(1).write()
                .mode("overwrite") // modo de escrita
                .format("csv") // tipo do arquivo de saída
                .option("header", "true") // com cabeçalho
                .save("output/overflow.csv"); // salva

        // parando a sessão
        session.stop();

    }

}
