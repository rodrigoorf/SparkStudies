package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HousePriceSQL {

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";

    public static void main(String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // lendo os dados do arquivo
        Dataset<Row> dados = session.read().option("header", "true").option("inferSchema", "true")
                .csv("in/RealEstate.csv");

        dados.groupBy(col("Location")) // agrupando por localização
                .agg(max("Price"), avg(col("Price SQ Ft"))) // buscando maior valor das casas e valor médio do pé quadrado das casas
                .sort(col("avg(Price SQ Ft)").desc()) // ordenando pelo valor médio do pé quadrado, do maior para o menor
                .show(); // mostrar na tela

        // valor médio do pé quadrado
        //dados.groupBy(col("Location"))

        session.stop();


    }
}
