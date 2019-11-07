package pairrdd.aggregation;

import java.io.Serializable;

public class AvgCount implements Serializable {

    private int ocorrencia;
    private double preco;

    public AvgCount() {
    }

    public AvgCount(int ocorrencia, double preco) {
        this.ocorrencia = ocorrencia;
        this.preco = preco;
    }

    public int getOcorrencia() {
        return ocorrencia;
    }

    public void setOcorrencia(int ocorrencia) {
        this.ocorrencia = ocorrencia;
    }

    public double getPreco() {
        return preco;
    }

    public void setPreco(double preco) {
        this.preco = preco;
    }

    @Override
    public String toString() {
        return "AvgCount{" +
                "ocorrencia=" + ocorrencia +
                ", preco=" + preco +
                '}';
    }
}
