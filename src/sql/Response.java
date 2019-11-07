package sql;

import java.io.Serializable;

public class Response implements Serializable {
    private String occupation;
    private Double ageMidpoint;
    private Double salaryMidpoint;

    public Response() {
    }

    public Response(String occupation, Double ageMidpoint, Double salaryMidpoint) {
        this.occupation = occupation;
        this.ageMidpoint = ageMidpoint;
        this.salaryMidpoint = salaryMidpoint;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public Double getAgeMidpoint() {
        return ageMidpoint;
    }

    public void setAgeMidpoint(Double ageMidpoint) {
        this.ageMidpoint = ageMidpoint;
    }

    public Double getSalaryMidpoint() {
        return salaryMidpoint;
    }

    public void setSalaryMidpoint(Double salaryMidpoint) {
        this.salaryMidpoint = salaryMidpoint;
    }
}
