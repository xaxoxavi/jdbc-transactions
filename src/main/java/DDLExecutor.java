import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by xavi on 2/05/18.
 */
public class DDLExecutor {

    private BasicDataSource pool;

    public DDLExecutor(BasicDataSource pool){
        this.pool = pool;
    }

    /**
     * Una taula producte, on tindrem els camps
     Id: autoincremental i sense negatius. Esperem tenir milions de productes.
     Nom: Nom del producte. No sol superar els 45 caracters
     Descripcio: Detalls del producte
     Stocprivate Connection connection;k: Unitats disponibles del producte. No podem tenir mai unitats negatives.
     Preu: Preu de venta del producte
     */

    private final String producteDDL = "CREATE TABLE IF NOT EXISTS producte (" +
                                        "id MEDIUMINT UNSIGNED AUTO_INCREMENT," +
                                        "nom VARCHAR(50)," +
                                        "descripcio TEXT," +
                                        "stock SMALLINT UNSIGNED DEFAULT 0," +
                                        "preu DECIMAL(13,2) UNSIGNED, " +
                                        "PRIMARY KEY (id) );";

    private final String facturaDDL = "CREATE TABLE IF NOT EXISTS factura (" +
                                      " id INT UNSIGNED AUTO_INCREMENT, " +
                                        " data TIMESTAMP DEFAULT current_timestamp, " +
                                        " total INT UNSIGNED, " +
                                        " PRIMARY KEY (id));";

    private final String cistellaDDL =  "CREATE TABLE IF NOT EXISTS cistella (" +
                                        "id INT UNSIGNED AUTO_INCREMENT, " +
                                        "nomClient VARCHAR(50), " +
                                        "numFactura INT UNSIGNED, " +
                                        "PRIMARY KEY (id), " +
                                        "FOREIGN KEY (numFactura) REFERENCES factura(id));";

    private final String cistellaProducteDDL =  "CREATE TABLE IF NOT EXISTS cistella_producte (" +
                                                "idProducte MEDIUMINT UNSIGNED, " +
                                                "idCistella INT UNSIGNED, " +
                                                "FOREIGN KEY (idProducte) REFERENCES producte (id), " +
                                                "FOREIGN KEY (idCistella) REFERENCES cistella (id), " +
                                                "PRIMARY KEY (idProducte, idCistella))";

    private final String facturaProducteDDL = "CREATE TABLE IF NOT EXISTS factura_producte (" +
                                              "numFactura INT UNSIGNED, " +
                                              "idProducte MEDIUMINT UNSIGNED, " +
                                              "FOREIGN KEY (idProducte) REFERENCES producte (id), " +
                                              "FOREIGN KEY (numFactura) REFERENCES factura (id), " +
                                              "PRIMARY KEY (numFactura,idProducte));";

    public void createTables(){

        Connection connection = null;

        try {
            connection = pool.getConnection();

            Statement statement = connection.createStatement();

            statement.executeUpdate("DROP SCHEMA IF EXISTS facturacio;");
            statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS facturacio;");
            statement.executeUpdate("USE facturacio;");

            statement.executeUpdate(this.producteDDL);
            statement.executeUpdate(this.facturaDDL);
            statement.executeUpdate(this.cistellaDDL);
            statement.executeUpdate(this.cistellaProducteDDL);
            statement.executeUpdate(this.facturaDDL);
            statement.executeUpdate(this.facturaProducteDDL);




        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }





}
