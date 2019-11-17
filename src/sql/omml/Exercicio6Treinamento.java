package sql.omml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
// percentual de propostas de crédito cujo cliente vive em uma cidada com IDH de acordo com as faixas
public class Exercicio6Treinamento {
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

        // removendo valores indesejáveis
        Dataset<Row> filtrado = arquivo.filter(col("IDHMUNICIPIO").$greater$eq(0));

        double total = filtrado.count();

        // 0-10
        double cat1 = filtrado.filter(col("IDHMUNICIPIO").between(0, 10)).count();
        System.out.println("0-10: " + calcularPorcentagem(cat1, total) + "%");

        // 10-20
        double cat2 = filtrado.filter(col("IDHMUNICIPIO").between(11, 20)).count();
        System.out.println("10-20: " + calcularPorcentagem(cat2, total) + "%");

        // 20-30
        double cat3 = filtrado.filter(col("IDHMUNICIPIO").between(21, 30)).count();
        System.out.println("20-30: " + calcularPorcentagem(cat3, total) + "%");

        // 30-40
        double cat4 = filtrado.filter(col("IDHMUNICIPIO").between(31, 40)).count();
        System.out.println("30-40: " + calcularPorcentagem(cat4, total) + "%");

        // 40-50
        double cat5 = filtrado.filter(col("IDHMUNICIPIO").between(41, 50)).count();
        System.out.println("40-50: " + calcularPorcentagem(cat5, total) + "%");

        // 50-60
        double cat6 = filtrado.filter(col("IDHMUNICIPIO").between(51, 60)).count();
        System.out.println("50-60: " + calcularPorcentagem(cat6, total) + "%");

        // 60-70
        double cat7 = filtrado.filter(col("IDHMUNICIPIO").between(61, 70)).count();
        System.out.println("60-70: " + calcularPorcentagem(cat7, total) + "%");

        // 70-80
        double cat8 = filtrado.filter(col("IDHMUNICIPIO").between(71, 80)).count();
        System.out.println("70-80: " + calcularPorcentagem(cat8, total) + "%");

        // 80-90
        double cat9 = filtrado.filter(col("IDHMUNICIPIO").between(81, 90)).count();
        System.out.println("80-90: " + calcularPorcentagem(cat9, total) + "%");

        // 90-100
        double cat10 = filtrado.filter(col("IDHMUNICIPIO").between(91, 100)).count();
        System.out.println("90-100: " + calcularPorcentagem(cat10, total) + "%");

        // parando a sessão
        session.stop();
    }

    public static double calcularPorcentagem(double valor, double total){
        return (valor / total) * 100;
    }
}
