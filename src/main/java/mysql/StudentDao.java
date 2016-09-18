package mysql;

import exception.DaoException;
import dto.Student;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class StudentDao extends AbstractGenericDao<Student> {

    public StudentDao(Connection connection) throws DaoException {
        super(connection);
    }


    @Override
    public void update(Student student) throws DaoException {

        try {
            PreparedStatement statement = getPreparedStatement(UPDATE_STUDENT);

            statement.setString(1, student.getName());
            statement.setString(2, student.getSurname());
            statement.setInt(3, student.getId());

            int count = statement.executeUpdate();
            if (count != 1) {
                throw new DaoException("update more then 1 record: " + count);
            }
        } catch (SQLException e) {
            throw new DaoException("update failed! ", e);
        }
    }

    @Override
    public void add(Student student) throws DaoException {

        try {
            PreparedStatement statement = getPreparedStatement(ADD_STUDENT);

            statement.setString(1, student.getName());
            statement.setString(2, student.getSurname());

            int count = statement.executeUpdate();
            if (count != 1) {
                throw new DaoException("add more then 1 record: " + count);
            }
        } catch (SQLException e) {
            throw new DaoException("add failed! ", e);
        }
    }

    @Override
    public void delete(Student student) throws DaoException {

        try {
            PreparedStatement statement = getPreparedStatement(DELETE_STUDENT);

            statement.setInt(1, student.getId());

            int count = statement.executeUpdate();
            if (count != 1) {
                throw new DaoException("delete more then 1 record: " + count);
            }
        } catch (SQLException e) {
            throw new DaoException("delete failed! ", e);
        }
    }

    @Override
    public List<Student> getAll() throws DaoException {

        LinkedList<Student> result = new LinkedList<>();

        try {
            PreparedStatement statement = getPreparedStatement(GET_ALL_STUDENT);

            try (ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    Student student = new Student();
                    student.setId(rs.getInt("id"));
                    student.setName(rs.getString("name"));
                    student.setSurname(rs.getString("surname"));
                    result.add(student);
                }
            }
        } catch (SQLException e) {
            throw new DaoException("get all failed! ", e);
        }
        return result;
    }

    @Override
    public Student getById(Integer id) throws DaoException {

        Student student = null;

        try {
            PreparedStatement statement = getPreparedStatement(GET_STUDENT_BY_ID);
            statement.setInt(1, id);

            try (ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    student = new Student();
                    student.setId(rs.getInt("id"));
                    student.setName(rs.getString("name"));
                    student.setSurname(rs.getString("surname"));
                }
            }
        } catch (SQLException e) {
            throw new DaoException("get all byId failed! ", e);
        }
        return student;
    }

    //прочитать всех студентов
    public void readAllStudent() throws DaoException {

        LinkedList<Student> result = new LinkedList<>();

        try {
            PreparedStatement statement = getPreparedStatement(GET_ALL_STUDENT);

            try (ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    Student student = new Student();
                    student.setId(rs.getInt("id"));
                    student.setName(rs.getString("name"));
                    student.setSurname(rs.getString("surname"));
                    result.add(student);
                }
            }
            for (Iterator<Student> iterator = result.iterator(); iterator.hasNext(); ) {
                System.out.println(iterator.next());
            }
        } catch (SQLException e) {
            throw new DaoException("read all student failed! ", e);
        }
    }
}
