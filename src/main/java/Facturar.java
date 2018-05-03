import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;

/**
 * Created by xavi on 2/05/18.
 */
public class Facturar {

    private BasicDataSource pool;

    private final String initBill = "INSERT INTO factura (id,data) VALUES (default,now())";

    private final String updateBillId = "UPDATE cistella SET numFactura = ? WHERE id = ?";

    private final String basketProducts = "SELECT cistella_producte.idCistella,producte.id, producte.stock, producte.preu" +
            "   FROM producte INNER JOIN cistella_producte  " +
            "   ON producte.id = cistella_producte.idProducte " +
            "   WHERE cistella_producte.idCistella = ?";

    private final String toBill = "INSERT INTO factura_producte (numFactura,idProducte) VALUES (?,?)";

    private final String updateStock = "UPDATE producte SET stock = stock-1 WHERE id = ?";
    private final String updateBillImport = "UPDATE factura SET total = ? WHERE id = ?";

    private final String selectBaskets = "SELECT id FROM cistella";


    private PreparedStatement billImportPstmt;
    private PreparedStatement basketPstmt;
    private PreparedStatement facturarStmt;
    private PreparedStatement stockStmt;

    public Facturar(BasicDataSource pool) {
        this.pool = pool;
    }


    public void doBillingProcess() {
        /**
         * He tengut que pujar la gestió de la configuració a un mètode superior, perquè hem de poder
         * gestionar les transaccions globals.
         * */

        Connection connection = null;

        try {
            connection = pool.getConnection();

            Statement statement = connection.createStatement();
            ResultSet baskets = statement.executeQuery(selectBaskets);

            connection.setAutoCommit(false);

            //Inicialitzam Prepared Statmenes un pic (després les reutilitzam).
            billImportPstmt = connection.prepareStatement(updateBillImport);
            basketPstmt = connection.prepareStatement(basketProducts);
            facturarStmt = connection.prepareStatement(toBill);
            stockStmt = connection.prepareStatement(updateStock);

            while (baskets.next()) {
                Integer idCistella = baskets.getInt("id");

                Savepoint savepoint = connection.setSavepoint();

                try {
                    Integer idFactura = initBill(idCistella, connection);

                    Double importTotalFactura = checkBill(idCistella, idFactura, connection);

                    updateBillImport(idFactura, importTotalFactura, connection);

                    connection.releaseSavepoint(savepoint);

                } catch (ReturnToSavePointException rtspe) {

                    connection.rollback(savepoint);

                }
            }


        } catch (Exception e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }


        } finally {

            if (connection != null) {

                try {

                    Statement statement = connection.createStatement();
                    connection.commit();
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }


            }
        }


    }

    public void batchData() throws SQLException {

        Connection connection = pool.getConnection();

        Statement statement = connection.createStatement();

        statement.execute("INSERT INTO producte (nom,descripcio,stock,preu) VALUES ('pollastre','ecologoc',3,10.23);");
        statement.execute("INSERT INTO producte (nom,descripcio,stock,preu) VALUES ('porro','ecologogic',1,1.23);");
        statement.execute("INSERT INTO producte (nom,descripcio,stock,preu) VALUES ('pastanaga','ecologogica',1,2.43);");
        statement.execute("INSERT INTO producte (nom,descripcio,stock,preu) VALUES ('ceba','ecologogica',1,1.10);");
        statement.execute("INSERT INTO producte (nom,descripcio,stock,preu) VALUES ('alls','anti vampirs',1,0.70);");

        statement.execute("INSERT INTO cistella (nomClient) VALUES ('Pep Buades');");
        statement.execute("INSERT INTO cistella (nomClient) VALUES ('Pere Negre');");
        statement.execute("INSERT INTO cistella (nomClient) VALUES ('Joan Galmés');");

        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (1,1);");
        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (2,1);");
        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (3,1);");

        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (2,2);");
        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (3,2);");
        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (4,2);");

        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (1,3);");
        statement.execute("INSERT INTO cistella_producte (idProducte,idCistella) VALUES (5,3);");


    }

    private Integer initBill(int idCistella, Connection connection) throws SQLException {


        Statement statement = connection.createStatement();

        statement.execute(initBill);

        ResultSet resultSet = statement.executeQuery("SELECT @@IDENTITY");

        if (resultSet.next()) {

            Integer idFactura = resultSet.getInt(1);

            PreparedStatement preparedStatement = connection.prepareStatement(updateBillId);

            preparedStatement.setInt(1, idFactura);
            preparedStatement.setInt(2, idCistella);

            preparedStatement.execute();

            return idFactura;

        }

        return null;

    }

    private Double checkBill(int idCistella, int idFactura, Connection connection) throws ReturnToSavePointException, SQLException {

        Double totalFactura = 0D;

        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();

        statement.execute("LOCK TABLES producte WRITE, cistella_producte WRITE, cistella WRITE, factura_producte WRITE;");

        try {

            basketPstmt.setInt(1, idCistella);
            ResultSet rs = basketPstmt.executeQuery();

            // !! Ferem el càlcul del total de la factura aquí, per tal d'estalviar-nos una query.

            Integer productesFacturats = 0;
            while (rs.next()) {

                Integer idProducte = rs.getInt(2);
                Integer stock = rs.getInt(3);

                if (stock >= 1) {

                    facturarStmt.setInt(1, idFactura);
                    facturarStmt.setInt(2, idProducte);
                    facturarStmt.execute();

                    stockStmt.setInt(1, idProducte);
                    stockStmt.execute();

                    totalFactura += rs.getDouble(4);
                    productesFacturats++;

                }

            }

            if (productesFacturats == 0) {

                throw new ReturnToSavePointException();
            }

        } finally {
            statement.execute("UNLOCK TABLES");
        }


        return totalFactura;


    }

    private void updateBillImport(Integer numFactura, Double importTotalFactura, Connection connection) throws SQLException {


        billImportPstmt.setDouble(1, importTotalFactura);
        billImportPstmt.setInt(2, numFactura);

        billImportPstmt.execute();

    }
}
