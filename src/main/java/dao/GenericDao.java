package dao;

import dto.Identified;
import exception.DaoException;

import java.util.List;


public interface GenericDao<T extends Identified> {


    String GET_ALL_STUDENT = "SELECT id, name, surname FROM student ";
    String GET_STUDENT_BY_ID = "SELECT id, name, surname FROM student WHERE id = ?";
    String ADD_STUDENT = "INSERT INTO student (name, surname) values (?, ?)";
    String UPDATE_STUDENT = "UPDATE student SET name = ?, surname = ? WHERE id= ?";
    String DELETE_STUDENT = "DELETE FROM student WHERE id= ?";

    String GET_ALL_SUBJECT = "SELECT id, name FROM subject";
    String GET_SUBJECT_BY_ID = "SELECT id, name FROM subject WHERE id = ?";
    String GET_ALL_SUBJECT_OF_STUDENT = "SELECT S.id, S.name FROM mark M INNER JOIN subject S " +
            "ON M.subject_id = S.id WHERE M.student_id = ?";
    String ADD_SUBJECT = "INSERT INTO subject (name) values (?)";
    String UPDATE_SUBJECT = "UPDATE subject SET name = ? WHERE id= ?";
    String DELETE_SUBJECT = "DELETE FROM subject WHERE id= ?";

    String GET_ALL_MARK = "SELECT id, mark, student_id, subject_id FROM mark";
    String GET_MARK_BY_ID = "SELECT id, mark, student_id, subject_id FROM mark WHERE id = ?";
    String GET_ALL_MARK_OF_STUDENT = "SELECT id, mark, student_id, subject_id FROM mark WHERE student_id = ?";
    String ADD_MARK = "INSERT INTO mark (mark, student_id, subject_id) values (?,?,?)";
    String UPDATE_MARK = "UPDATE mark SET mark = ?, student_id = ?, subject_id = ? WHERE id = ?";
    String DELETE_MARK = "DELETE FROM mark WHERE id = ?";


    void update(T object) throws DaoException;

    void add(T object) throws DaoException;

    void delete(T object) throws DaoException;

    List<T> getAll() throws DaoException;

    T getById(Integer id) throws DaoException;
}
