import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;

/**
 * Created by xavi on 2/05/18.
 */
public class Facturar {

    private BasicDataSource pool;

    private final String initBill = "INSERT INTO factura (id,dataFacturacio) VALUES (default,now())";

    private final String updateBillId = "UPDATE cistella SET numFactura = ? WHERE id = ?";

    private final String basketProducts = "SELECT cistella.id,producte.id, producte.stock, producte.preu" +
            "   FROM producte INNER JOIN cistella_producte  " +
            "   ON producte.id = cistella_producte.idProducte " +
            "   WHERE cistella_producte.id = ?";

    private final String toBill = "INSERT INTO factura_producte (numFactura,numProducte) VALUES (?,?)";

    private final String updateStock = "UPDATE producte SET stock = stock-1 WHERE id = ?";
    private final String updateBillImport = "UPDATE factura SET total = ? WHERE id = ?";


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

            connection.setAutoCommit(false);

            //Inicialitzam Prepared Statmenes un pic (després les reutilitzam).
            billImportPstmt = connection.prepareStatement(updateBillImport);
            basketPstmt = connection.prepareStatement(basketProducts);
            facturarStmt = connection.prepareStatement(toBill);
            stockStmt = connection.prepareStatement(updateStock);

            Integer idCistella = 1;

            Savepoint savepoint = connection.setSavepoint();

            try {
                Integer idFactura = initBill(idCistella, connection);

                Double importTotalFactura = checkBill(idCistella, idFactura, connection);

                updateBillImport(idFactura, importTotalFactura, connection);

            } catch (ReturnToSavePointException rtspe) {

                connection.rollback(savepoint);

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
                    statement.execute("UNLOCK TABLES");
                    connection.commit();
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }


            }
        }


    }

    private void batchData(Connection connection) throws SQLException {

        Statement statement = connection.createStatement();

        statement.execute("INSERT ");



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

        statement.execute("LOCK TABLES producte WRITE, cistella_producte WRITE, cistella WRITE;");


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

                if (facturarStmt.execute()) {

                    stockStmt.setInt(1, idProducte);
                    //TODO: Millor una execpecio custom
                    if (!stockStmt.execute()) throw new ReturnToSavePointException("ERROR AL ACTUALITZAR!");
                    totalFactura += rs.getDouble(4);
                    productesFacturats++;
                }
            }

        }

        if (productesFacturats == 0) {

            throw new ReturnToSavePointException();
        }


        return totalFactura;


    }

    private void updateBillImport(Integer numFactura, Double importTotalFactura, Connection connection) throws SQLException {


        billImportPstmt.setDouble(1, importTotalFactura);
        billImportPstmt.setInt(2, numFactura);

        billImportPstmt.execute();

    }
}
