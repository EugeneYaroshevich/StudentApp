package dao;

import exception.DaoException;
import mysql.DaoFactoryMySql;
import mysql.MarkDao;
import mysql.StudentDao;
import mysql.SubjectDao;


public abstract class DaoFactory implements AutoCloseable {

    public static <T extends DaoFactory> T getDAOFactory() throws DaoException {
        return (T) new DaoFactoryMySql();
    }


    public abstract StudentDao getStudentDao() throws DaoException;

    public abstract SubjectDao getSubjectDao() throws DaoException;

    public abstract MarkDao getMarkDao() throws DaoException;
}
