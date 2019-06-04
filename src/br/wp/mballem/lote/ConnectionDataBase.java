/* 
 * Autor: Marcio Ballem
 * Arigo: sobre inserção da dados em lote (bath) 
 * http://www.mballem.com/post/jdbc-bath-insercoes-em-lote-com-jdbc/?i=1
 * 
 * Codigo do artigo alterado p/:
 *     - conexão SQL SERVER usando jTDS 1.3, no meu caso em um SQL SERVER em conexão local
 *     - variavel final para indicar o nome da tabela de usuario a ser criada: tabelaUsr
 *     - o arquivo de carga de dados passa ter o nome da tabelaUsr;
 *     - criado condição para fazer Creeate Table na sintaxe do SQL Server
 *     - criado condição para fazer INSERT na sintaxe do SQL Server
 *     - alterado metodos para passar o banco da conexao :  - createTable()
 *                                                          - gerarArquivoSQL                                                   
 */             

package br.wp.mballem.lote;

import java.sql.*;

public class ConnectionDataBase {
	
	
	//P/ conectar MYSQL tire os comentario abaixo, substitua tabela e comente SQLServer 
    //private static final String URL = "jdbc:mysql://localhost/agenda";
    //private static final String DRIVER = "com.mysql.jdbc.Driver";
	                                  
	//P/ conectar SQL SERVER - por como comentario abaixo se for usar MYSQL
	private static final String URL = "jdbc:jtds:sqlserver://localhost:1433;databaseName=MAXPRODAD";
	private static final String DRIVER = "net.sourceforge.jtds.jdbc.Driver";
	
    private static final String USER = "sa";                                    //seu usuario
    private static final String PASS = "12345";                                 //sua senha

    public static Connection getConnection() {
        System.out.println("Conectando ao Banco de Dados");
        try {
            Class.forName(DRIVER);
            return DriverManager.getConnection(URL, USER, PASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        try {
            if (conn!= null) {
                conn.close();
            }

            if (stmt!= null) {
                stmt.close();
            }

            if (rs!= null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String nomeBanco) {
        Connection connection = getConnection();
        PreparedStatement stmt = null;
        String sqlScript="";
        
        if (nomeBanco.equals("MYSQL")) {
        	sqlScript = 
        		" CREATE TABLE IF NOT EXISTS " +Teste.tabelaUsr + " (" +
                "  ID_USER bigint(20) NOT NULL AUTO_INCREMENT," +
                "  FIRST_NAME VARCHAR(255) NOT NULL," +
                "  SURNAME VARCHAR(255) NOT NULL," +
                "  AGE INT NOT NULL," +
                "  EMAIL VARCHAR(255) NOT NULL," +
                "  CONSTRAINT " + "PK_"+ Teste.tabelaUsr+" PRIMARY KEY (ID_USER)" +
                ");";
        }else if (nomeBanco.equals("SQL")){
        	sqlScript =
        		"	if object_id('" +Teste.tabelaUsr+ "' ) is null " +
    			"	begin " +
    			"     CREATE TABLE "+Teste.tabelaUsr+ " (" +
        		"     ID_USER bigint NOT NULL IDENTITY, " +
        		"     FIRST_NAME VARCHAR(255) NOT NULL, " +
        		"     SURNAME VARCHAR(255) NOT NULL, " +
        		"     AGE INT NOT NULL, " +
        		"     EMAIL VARCHAR(255) NOT NULL, " +
        		"     CONSTRAINT " + "PK_"+Teste.tabelaUsr+" PRIMARY KEY (ID_USER)) " +
        		"	end;";  	
        }
        
        
        try {
            stmt = connection.prepareStatement(sqlScript);
            stmt.execute();
            System.out.println("Create Tables Ok!");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(connection, stmt, null);
        }
    }
}