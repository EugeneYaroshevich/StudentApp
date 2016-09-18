package controller;

import exception.DaoException;
import mysql.DaoFactoryMySql;
import mysql.MarkDao;
import mysql.StudentDao;
import mysql.SubjectDao;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

public class StudentServletSessionListener implements HttpSessionListener {

    HttpSession httpSession;

    @Override
    public void sessionCreated(HttpSessionEvent sessionEvent) {

        try {
            DaoFactoryMySql daoFactory = DaoFactoryMySql.getDAOFactory();
            httpSession = sessionEvent.getSession();
            httpSession.setAttribute("StudentDao", daoFactory.getStudentDao());
            httpSession.setAttribute("SubjectDao", daoFactory.getSubjectDao());
            httpSession.setAttribute("MarkDao", daoFactory.getMarkDao());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sessionDestroyed(HttpSessionEvent sessionEvent) {

        StudentDao studentDao = (StudentDao) httpSession.getAttribute("StudentDao");
        SubjectDao subjectDao = (SubjectDao) httpSession.getAttribute("SubjectDao");
        MarkDao markDao = (MarkDao) httpSession.getAttribute("MarkDao");
        try {
            if (studentDao != null) {
                studentDao.close();
            }
            if (subjectDao != null) {
                subjectDao.close();
            }
            if (markDao != null){
                markDao.close();
            }
        } catch (DaoException e) {
            e.printStackTrace();
        }
    }
}
