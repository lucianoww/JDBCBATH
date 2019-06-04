# JDBCBATH
Script SQL de Insert em modo Bath nos Bancos de Dados: MYSQL e SQL SERVER

Baseado no artigo do Marcelo Ballem:
http://www.mballem.com/post/jdbc-bath-insercoes-em-lote-com-jdbc/?i=1

De novo, em relação ao artigo do Ballem:
- implementação da conexao SQL SERVER através do jTDS 1.3
- variavel final com nome da tabela de usuários que será criada, e não mais fixa;
- gravação do arquivo com os scripts de dados para modo bath com o nome da tabela;
- criação do script para criação da tabela de usuários conforme sintaxe do SQL SERVER;
- criação do script de insert com persistência de colunas conforme sintaxe do SQL SERVER;
