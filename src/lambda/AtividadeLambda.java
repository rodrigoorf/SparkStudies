package lambda;

import java.util.ArrayList;
import java.util.stream.Stream;

public class AtividadeLambda {
    public static void main(String[] args) {
        // criando lista
        ArrayList<Pessoa> pessoas = new ArrayList<Pessoa>();
        // adicionando pessoas
        pessoas.add(new Pessoa("Fulano", 15));
        pessoas.add(new Pessoa("Beltrano", 25));
        pessoas.add(new Pessoa("Ciclano", 35));
        // imprimindo todas
        pessoas.forEach(p -> System.out.println("Nome: " + p.getNome() + " | Idade: " + p.getIdade()));
        // imprimindo com idade maior que 20 apenas
        pessoas.forEach(p -> {
            if(p.getIdade() > 20){
                System.out.println("Nome: " + p.getNome() + " | Idade: " + p.getIdade());
            }
        });
        // verificar por nome
        boolean hasPessoa = pessoas.stream().anyMatch(pessoa -> pessoa.getNome().contains("Fulano"));
        System.out.println(hasPessoa);
    }
}
