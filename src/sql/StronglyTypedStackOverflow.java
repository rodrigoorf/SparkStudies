package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class StronglyTypedStackOverflow {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // carregando os dados
        Dataset<Row> original = session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("in/2016-stack-overflow-survey-responses.csv");

        // obtendo apenas as colunas de interesse e renomeando colunas de acordo com Response.java
        Dataset<Row> selecionado =
                original.select(col("occupation"),
                        col("age_midpoint").as("ageMidpoint"),
                        col("salary_midpoint").as("salaryMidpoint"));

        // verificando tabela
        //selecionado.show();

        // convertendo de Row para Response
        Dataset<Response> convertido = selecionado.as(Encoders.bean(Response.class));

        // filtro para "program" no nome da ocupação
        Dataset<Response> filtrado = convertido.filter(v -> v.getOccupation() != null && v.getOccupation().contains("program"));

        filtrado.show();

        // parando a sessão
        session.stop();

    }

}
