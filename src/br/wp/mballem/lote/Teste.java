package br.wp.mballem.lote;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Teste {

	private final Integer numeroRegistros=20000;
	
	public static String tabelaUsr = "maxusers";
	
    private long start;

    private static final String INSERT_MYSQL =
            "insert into " +Teste.tabelaUsr+ " (FIRST_NAME, SURNAME, AGE, EMAIL) " +
            "values ('Ana Maria', 'de Souza', 30, 'ana@email.com');";
    
    private static final String INSERT_SQLSERVER =
            "insert into " +Teste.tabelaUsr+ " (FIRST_NAME, SURNAME, AGE, EMAIL) " +
            "values ( 'Ana Maria', 'de Souza', 30, 'ana@email.com' ) ;";

    public static void main(String[] args) {
    	//
    	String nomeBD="SQL";
    	
        ConnectionDataBase.createTable(nomeBD);

        new Teste().gerarArquivoSQL(nomeBD);

        new Teste().save();

        new Teste().saveBatch();
    }

    private void gerarArquivoSQL(String nomeBanco) {
        
    	String myInsert=null;
    	File file=new File("c:\\temp\\"+Teste.tabelaUsr+"_insert.sql");
        
        
        if (nomeBanco.equals("MYSQL"))
        	myInsert=INSERT_MYSQL;
        else if (nomeBanco.equals("SQL"))
        	myInsert=INSERT_SQLSERVER;
        	
        
        try {
            file.createNewFile();
            FileWriter fileWriter = new FileWriter(file, true);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            for (int i = 0; i < numeroRegistros; i++) {
                printWriter.println(myInsert);
            }
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private List lerArquivoSQL() {
        File file = new File("c:\\temp\\"+Teste.tabelaUsr+"_insert.sql");
        List inserts = new ArrayList();
        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String linha = "";
            while ((linha = bufferedReader.readLine()) != null) {
                inserts.add(linha);
            }
            fileReader.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inserts;
    }

    private void save() {
        //Abre a conexao
        Connection conn = ConnectionDataBase.getConnection();
        Statement stmt = null;
        List <String> list = new ArrayList();
        //Cria um lista para receber os inserts do arquivo
        list = lerArquivoSQL();
        try {
            //inicializa o objeto statement
            stmt = conn.createStatement();
            start = System.currentTimeMillis();
            for (String str : list) {
                stmt.execute(str);
            }

            calculaTempo(System.currentTimeMillis() - start);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionDataBase.close(conn, stmt, null);
        }
    }

    private void saveBatch() {
        //Abre a conexao
        Connection conn = ConnectionDataBase.getConnection();
        Statement stmt = null;
        List <String> list = new ArrayList();
        //Cria um lista para receber os inserts do arquivo
        list = lerArquivoSQL();
        try {
            //inicializa o objeto statement
            stmt = conn.createStatement();
            start = System.currentTimeMillis();
            //faz um for na lista e adiciona no método addBatch()
            // cada insert que veio do arquivo
            for (String str : list) {
                stmt.addBatch(str);
            }
            //faz o insert em lote no banco pelo método executeBatch()
            stmt.executeBatch();
            //limpa o objeto stmt
            stmt.clearBatch();

            calculaTempo(System.currentTimeMillis() - start);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionDataBase.close(conn, stmt, null);
        }

    }

    private void calculaTempo(long time) {
        long sec = time / 1000;
        long min = time / (60 * 1000);
        long hour = time / (60 * 60 * 1000);

        if (hour > 0) {
            System.out.println("Total da operacao " + hour + "hs");
        } else if (min > 0) {
            System.out.println("Total da operacao " + min + "min");
        } else if (sec > 0) {
            System.out.println("Total da operacao " + sec + "s");
        }
    }
}