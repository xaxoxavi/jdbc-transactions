import org.apache.commons.dbcp2.BasicDataSource;

/**
 * Created by xavi on 2/05/18.
 */
public class Main {

    private static final java.lang.String CADENA_CONNEXIO = "jdbc:mysql://localhost/facturacio?createDatabaseIfNotExist=true&useServerPrepStmts=true";
    private static final String USER_BBDD = "root";
    private static final String PASS_BBDD = "test";


    private static BasicDataSource pool;

    public static void main(String[] args) throws Exception {

        pool = new BasicDataSource();
        pool.setDriverClassName("com.mysql.jdbc.Driver");
        pool.setUsername(USER_BBDD);
        pool.setPassword(PASS_BBDD);
        pool.setUrl(CADENA_CONNEXIO);

        // Opcional. Sentencia SQL que le puede servir a BasicDataSource
        // para comprobar que la conexion es correcta.
        pool.setValidationQuery("select 1");


        DDLExecutor ddlExecutor = new DDLExecutor(pool);

        ddlExecutor.createTables();

        Facturar facturar = new Facturar(pool);

        facturar.batchData();
        facturar.doBillingProcess();





    }

}
