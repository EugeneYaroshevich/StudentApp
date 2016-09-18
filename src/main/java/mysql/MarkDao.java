package mysql;

import exception.DaoException;
import dto.Mark;
import dto.Student;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;


public class MarkDao extends AbstractGenericDao<Mark> {

    public MarkDao(Connection connection) throws DaoException {
        super(connection);
    }


    @Override
    public void update(Mark mark) throws DaoException {

        try {
            PreparedStatement statement = getPreparedStatement(UPDATE_MARK);

            statement.setInt(1, mark.getMark());

            if (mark.getStudentId() == null) {
                statement.setNull(2, Types.INTEGER);
            } else {
                statement.setInt(2, mark.getStudentId());
            }

            if (mark.getSubjectId() == null) {
                statement.setNull(3, Types.INTEGER);
            } else {
                statement.setInt(3, mark.getSubjectId());
            }

            statement.setInt(4, mark.getId());

            int count = statement.executeUpdate();
            if (count != 1) {
                throw new DaoException("update more then 1 record: " + count);
            }
        } catch (SQLException e) {
            throw new DaoException("update failed! ", e);
        }
    }

    @Override
    public void add(Mark mark) throws DaoException {

        try {
            PreparedStatement statement = getPreparedStatement(ADD_MARK);

            statement.setInt(1, mark.getMark());

            if (mark.getStudentId() == null) {
                statement.setNull(2, Types.INTEGER);
            } else {
                statement.setInt(2, mark.getStudentId());
            }

            if (mark.getSubjectId() == null) {
                statement.setNull(3, Types.INTEGER);
            } else {
                statement.setInt(3, mark.getSubjectId());
            }


            int count = statement.executeUpdate();
            if (count != 1) {
                throw new DaoException("add more then 1 record: " + count);
            }
        } catch (SQLException e) {
            throw new DaoException("add failed! ", e);
        }
    }

    @Override
    public void delete(Mark mark) throws DaoException {

        try {
            PreparedStatement statement = getPreparedStatement(DELETE_MARK);

            statement.setInt(1, mark.getId());
            int count = statement.executeUpdate();
            if (count != 1) {
                throw new DaoException("delete more then 1 record: " + count);
            }
        } catch (Exception e) {
            throw new DaoException("delete failed! ", e);
        }
    }

    @Override
    public List<Mark> getAll() throws DaoException {

        LinkedList<Mark> result = new LinkedList<>();

        try {
            PreparedStatement statement = getPreparedStatement(GET_ALL_MARK);

            try (ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    Mark mark = new Mark();

                    mark.setId(rs.getInt("id"));
                    mark.setMark(rs.getInt("mark"));
                    mark.setStudentId(rs.getInt("student_id"));
                    mark.setSubjectId(rs.getInt("subject_id"));
                    result.add(mark);
                }
            }
        } catch (SQLException e) {
            throw new DaoException("get all failed! ", e);
        }
        return result;
    }

    @Override
    public Mark getById(Integer id) throws DaoException {

        Mark mark = null;

        try {
            PreparedStatement statement = getPreparedStatement(GET_MARK_BY_ID);
            statement.setInt(1, id);

            try (ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    mark = new Mark();
                    mark.setId(rs.getInt("id"));
                    mark.setMark(rs.getInt("mark"));
                    mark.setStudentId(rs.getInt("student_id"));
                    mark.setSubjectId(rs.getInt("subject_id"));
                }
            }
        } catch (SQLException e) {
            throw new DaoException("get by id failed! ", e);
        }
        return mark;
    }

    public List<Mark> getAllMarkOfStudent(Student student) throws DaoException {

        LinkedList<Mark> result = new LinkedList<>();

        try {
            PreparedStatement statement = getPreparedStatement(GET_ALL_MARK_OF_STUDENT);
            statement.setInt(1, student.getId());

            try (ResultSet rs = statement.executeQuery()) {

                while (rs.next()) {
                    Mark mark = new Mark();

                    mark.setId(rs.getInt("id"));
                    mark.setMark(rs.getInt("mark"));
                    mark.setStudentId(rs.getInt("student_id"));
                    mark.setSubjectId(rs.getInt("subject_id"));
                    result.add(mark);
                }
            }
        } catch (SQLException e) {
            throw new DaoException("get all mark of student failed! ", e);
        }
        return result;
    }
}
