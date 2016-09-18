package mysql;

import dao.DaoFactory;
import exception.DaoException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DaoFactoryMySql extends DaoFactory {

    private String user;
    private String password;
    private String url;
    private String driver;

    private Connection connection;

    public DaoFactoryMySql() throws DaoException {
        try {
            loadProperties();
            Class.forName(driver);
            this.connection = createConnection();

        } catch (ClassNotFoundException e) {
            throw new DaoException("Can not find a driver ",e);
        }
    }

    private Connection createConnection() throws DaoException {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new DaoException("Connection is not established! Check the login email and password, please! ",e);
        }
    }

    private void loadProperties() throws DaoException {

        Properties properties = new Properties();

        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("mysql.properties")){
            properties.load(in);
            driver = properties.getProperty("driver");
            url = properties.getProperty("url");
            user = properties.getProperty("user");
            password = properties.getProperty("password");

        } catch (FileNotFoundException e) {
            throw new DaoException("Can not find properties file ",e);
        } catch (IOException e2) {
            throw new DaoException(e2);
        }
    }


    public StudentDao getStudentDao() throws DaoException {
        return new StudentDao(connection);
    }

    public SubjectDao getSubjectDao() throws DaoException {
        return new SubjectDao(connection);
    }

    public MarkDao getMarkDao() throws DaoException {
        return new MarkDao(connection);
    }

    @Override
    public void close() throws DaoException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new DaoException(e);
            }
        }
    }
}