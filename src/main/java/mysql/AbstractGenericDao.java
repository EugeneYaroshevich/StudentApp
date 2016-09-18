package mysql;

import dao.GenericDao;
import dto.Identified;
import exception.DaoException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public abstract class AbstractGenericDao<T extends Identified> implements GenericDao<T>, AutoCloseable {

    private Connection connection;

    private Map<String, PreparedStatement> preparedStatements = new HashMap<>();

    protected AbstractGenericDao(Connection connection) {
        this.connection = connection;
    }


    protected PreparedStatement getPreparedStatement(String sql) throws DaoException {

        PreparedStatement preparedStatement = preparedStatements.get(sql);

        if (preparedStatement == null) {
            try {
                preparedStatement = connection.prepareStatement(sql);
                preparedStatements.put(sql, preparedStatement);
            } catch (SQLException e) {
                throw new DaoException(e);
            }
        }
        return preparedStatement;
    }

    @Override
    public void close() throws DaoException {

        for (PreparedStatement preparedStatement : preparedStatements.values()) {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                throw new DaoException(e);
            }
        }
    }
}
