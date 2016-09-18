package controller;

import java.util.regex.*;
import dto.Mark;
import dto.Student;
import dto.Subject;
import exception.DaoException;
import mysql.MarkDao;
import mysql.StudentDao;
import mysql.SubjectDao;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.HashMap;


public class StudentServlet extends HttpServlet {


    private final String STUDENT_DAO = "StudentDao";
    private final String SUBJECT_DAO = "SubjectDao";
    private final String MARK_DAO = "MarkDao";


    private final String ADD_STUDENT = "/student.jsp";
    private final String RE_ADD_STUDENT = "/re_student.jsp";
    private final String EDIT_STUDENT = "/editStudent.jsp";
    private final String RE_EDIT_STUDENT = "/re_editStudent.jsp";
    private final String LIST_STUDENT = "/listStudent.jsp";


    private final String ADD_SUBJECT = "/subject.jsp";
    private final String RE_ADD_SUBJECT = "/re_subject.jsp";
    private final String EDIT_SUBJECT = "/editSubject.jsp";
    private final String RE_EDIT_SUBJECT = "/re_editSubject.jsp";
    private final String LIST_SUBJECT = "/listSubject.jsp";

    private final String ADD_MARK = "/mark.jsp";
    private final String RE_ADD_MARK = "/re_mark.jsp";
    private final String EDIT_MARK = "/editMark.jsp";
    private final String RE_EDIT_MARK = "/re_editMark.jsp";
    private final String LIST_MARK = "/listMark.jsp";

    private final String NAME_VALIDATE = "[а-яА-ЯёЁa-zA-Z]{1,20}";
    private final String MARK_VALIDATE = "(\\d)([0]?)";
    private final String ID_VALIDATE = "\\d+";



    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {


        try {
            request.setCharacterEncoding("UTF-8");
            response.setContentType("text/html; charset=UTF-8");
            HttpSession session = request.getSession();

            StudentDao studentDao = (StudentDao) session.getAttribute(STUDENT_DAO);
            SubjectDao subjectDao = (SubjectDao) session.getAttribute(SUBJECT_DAO);
            MarkDao markDao = (MarkDao) session.getAttribute(MARK_DAO);

            String action = request.getParameter("action");

            switch (action) {

                case "addStudent":
                    addStudent(request, studentDao, response);
                    break;
                case "deleteStudent":
                    deleteStudent(request, studentDao, response);
                    break;
                case "editStudent":
                    editStudent(request, studentDao, response);
                    break;
                case "saveEditStudent":
                    saveEditStudent(request, studentDao, response);
                    break;
                case "listStudent":
                    listStudent(request, studentDao, response);
                    break;
                case "student":
                    request.getRequestDispatcher(ADD_STUDENT).forward(request, response);
                    break;


                case "addSubject":
                    addSubject(request, subjectDao, response);
                    break;
                case "deleteSubject":
                    deleteSubject(request, subjectDao, response);
                    break;
                case "editSubject":
                    editSubject(request, subjectDao, response);
                    break;
                case "saveEditSubject":
                    saveEditSubject(request, subjectDao, response);
                    break;
                case "listSubject":
                    listSubject(request, subjectDao, response);
                    break;
                case "subject":
                    request.getRequestDispatcher(ADD_SUBJECT).forward(request, response);
                    break;


                case "addMark":
                    addMark(request, markDao, response);
                    break;
                case "deleteMark":
                    deleteMark(request, markDao, response);
                    break;
                case "editMark":
                    editMark(request, markDao, response);
                    break;
                case "saveEditMark":
                    saveEditMark(request, markDao, response);
                    break;
                case "listMark":
                    listMark(request, markDao, response);
                    break;
                case "mark":
                    request.getRequestDispatcher(ADD_MARK).forward(request, response);
                    break;

                default:
                    request.getRequestDispatcher(ADD_STUDENT).forward(request, response);
                    break;
            }

        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }



    //проверяет параметры ввода формы "Student"
    public HashMap<String, String> validateStudent(String nameStudent, String surnameStudent) {

        HashMap<String, String> errors = new HashMap<>();

        validateNameStudent(nameStudent, errors);
        validateSurnameStudent(surnameStudent, errors);

        return errors;
    }

    public void validateNameStudent(String nameStudent, HashMap<String, String> errors) {

        if (!Pattern.compile(NAME_VALIDATE).matcher(nameStudent).matches()) {
            errors.put("nameStudent", "Name must have between 2 and 20 characters.");
        }
    }

    public void validateSurnameStudent(String surnameStudent, HashMap<String, String> errors) {

        if (!Pattern.compile(NAME_VALIDATE).matcher(surnameStudent).matches()) {
            errors.put("surnameStudent", "Surname must have between 2 and 20 characters.");
        }
    }


    //проверяет параметры ввода формы "Subject"
    public HashMap<String, String> validateSubject(String nameSubject) {
        HashMap<String, String> errors = new HashMap<>();

        validateNameSubject(nameSubject, errors);

        return errors;
    }

    public void validateNameSubject(String nameSubject, HashMap<String, String> errors) {

        if (!Pattern.compile(NAME_VALIDATE).matcher(nameSubject).matches()) {
            errors.put("nameSubject", "Name must have between 2 and 20 characters.");
        }
    }


    //проверяет параметры ввода формы "Mark"
    public HashMap<String, String> validateMark(String mark_student, String studentId, String subjectId) {
        HashMap<String, String> errors = new HashMap<>();

        validateMarkStudent(mark_student, errors);
        validateStudentIdMark(studentId, errors);
        validateSubjectIdMark(subjectId, errors);

        return errors;
    }

    public void validateMarkStudent(String mark_student, HashMap<String, String> errors) {

        if (!Pattern.compile(MARK_VALIDATE).matcher(mark_student).matches()) {
            errors.put("mark_student", "Mark value can be only the whole positive number from 0 to 10");
        }
    }

    public void validateStudentIdMark(String studentId, HashMap<String, String> errors) {

        if (!Pattern.compile(ID_VALIDATE).matcher(studentId).matches()) {
            errors.put("studentId", "StudentId value can only be positive number existing in the list of students");
        }
    }

    public void validateSubjectIdMark(String subjectId, HashMap<String, String> errors) {

        if (!Pattern.compile(ID_VALIDATE).matcher(subjectId).matches()) {
            errors.put("subjectId", "SubjectId value can only be positive number existing in the list of subjects");
        }
    }



    public void addStudent(HttpServletRequest request, StudentDao studentDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        String name = request.getParameter("name");
        String surname = request.getParameter("surname");

        if (validateStudent(name, surname).isEmpty()) {

            Student student = new Student();
            student.setName(name);
            student.setSurname(surname);

            studentDao.add(student);
            listStudent(request, studentDao, response);

        } else {

            request.setAttribute("name", name);
            request.setAttribute("surname", surname);
            request.setAttribute("errors", validateStudent(name, surname));
            request.getRequestDispatcher(RE_ADD_STUDENT).forward(request, response);
        }
    }

    public void addSubject(HttpServletRequest request, SubjectDao subjectDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        String name = request.getParameter("name");

        if (validateSubject(name).isEmpty()) {

            Subject subject = new Subject();
            subject.setName(name);

            subjectDao.add(subject);
            listSubject(request, subjectDao, response);
        } else {

            request.setAttribute("name", name);
            request.setAttribute("errors", validateSubject(name));
            request.getRequestDispatcher(RE_ADD_SUBJECT).forward(request, response);
        }
    }

    public void addMark(HttpServletRequest request, MarkDao markDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        String mark_student = request.getParameter("mark");
        String studentId = request.getParameter("studentId");
        String subjectId = request.getParameter("subjectId");

        if (validateMark(mark_student, studentId, subjectId).isEmpty()) {

            Mark mark = new Mark();
            mark.setMark(Integer.parseInt(mark_student));
            mark.setStudentId(Integer.valueOf(studentId));
            mark.setSubjectId(Integer.valueOf(subjectId));

            markDao.add(mark);
            listMark(request, markDao, response);
        } else {

            request.setAttribute("mark_student", mark_student);
            request.setAttribute("studentId", studentId);
            request.setAttribute("subjectId", subjectId);
            request.setAttribute("errors", validateMark(mark_student, studentId, subjectId));
            request.getRequestDispatcher(RE_ADD_MARK).forward(request, response);
        }
    }


    public void editStudent(HttpServletRequest request, StudentDao studentDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Student student = studentDao.getById(Integer.parseInt(request.getParameter("id")));

        request.setAttribute("id", student.getId());
        request.setAttribute("name", student.getName());
        request.setAttribute("surname", student.getSurname());

        request.getRequestDispatcher(EDIT_STUDENT).forward(request, response);

    }

    public void editSubject(HttpServletRequest request, SubjectDao subjectDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Subject subject = subjectDao.getById(Integer.parseInt(request.getParameter("id")));

        request.setAttribute("id", subject.getId());
        request.setAttribute("name", subject.getName());

        request.getRequestDispatcher(EDIT_SUBJECT).forward(request, response);

    }

    public void editMark(HttpServletRequest request, MarkDao markDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Mark mark = markDao.getById(Integer.parseInt(request.getParameter("id")));

        request.setAttribute("id", mark.getId());
        request.setAttribute("mark", mark.getMark());
        request.setAttribute("studentId", mark.getStudentId());
        request.setAttribute("subjectId", mark.getSubjectId());

        request.getRequestDispatcher(EDIT_MARK).forward(request, response);
    }


    public void saveEditStudent(HttpServletRequest request, StudentDao studentDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Integer id = Integer.valueOf(request.getParameter("id"));
        String name = request.getParameter("name");
        String surname = request.getParameter("surname");

        if (validateStudent(name, surname).isEmpty()) {

            Student student = new Student();
            student.setId(id);
            student.setName(name);
            student.setSurname(surname);

            studentDao.update(student);

            request.setAttribute("student", student);

            listStudent(request, studentDao, response);

        } else {

            request.setAttribute("id", id);
            request.setAttribute("name", name);
            request.setAttribute("surname", surname);
            request.setAttribute("errors", validateStudent(name, surname));
            request.getRequestDispatcher(RE_EDIT_STUDENT).forward(request, response);
        }
    }

    public void saveEditSubject(HttpServletRequest request, SubjectDao subjectDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Integer id = Integer.valueOf(request.getParameter("id"));
        String name = request.getParameter("name");

        if (validateSubject(name).isEmpty()) {

            Subject subject = new Subject();
            subject.setId(id);
            subject.setName(name);

            subjectDao.update(subject);

            request.setAttribute("subject", subject);

            listSubject(request, subjectDao, response);
        } else {

            request.setAttribute("id", id);
            request.setAttribute("name", name);
            request.setAttribute("errors", validateSubject(name));
            request.getRequestDispatcher(RE_EDIT_SUBJECT).forward(request, response);
        }
    }

    public void saveEditMark(HttpServletRequest request, MarkDao markDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Integer id = Integer.valueOf(request.getParameter("id"));
        String mark_student = request.getParameter("mark");
        String studentId = request.getParameter("studentId");
        String subjectId = request.getParameter("subjectId");

        if (validateMark(mark_student, studentId, subjectId).isEmpty()) {

            Mark mark = new Mark();
            mark.setId(id);
            mark.setMark(Integer.parseInt(mark_student));
            mark.setStudentId(Integer.valueOf(studentId));
            mark.setSubjectId(Integer.valueOf(subjectId));

            markDao.update(mark);

            request.setAttribute("markEntity", mark);

            listMark(request, markDao, response);
        } else {

            request.setAttribute("id", id);
            request.setAttribute("mark_student", mark_student);
            request.setAttribute("studentId", studentId);
            request.setAttribute("subjectId", subjectId);
            request.setAttribute("errors", validateMark(mark_student, studentId, subjectId));
            request.getRequestDispatcher(RE_EDIT_MARK).forward(request, response);
        }
    }


    public void deleteStudent(HttpServletRequest request, StudentDao studentDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Student student = new Student();
        student.setId(Integer.valueOf(request.getParameter("id")));

        studentDao.delete(student);

        listStudent(request, studentDao, response);
    }

    public void deleteSubject(HttpServletRequest request, SubjectDao subjectDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Subject subject = new Subject();
        subject.setId(Integer.valueOf(request.getParameter("id")));

        subjectDao.delete(subject);

        listSubject(request, subjectDao, response);
    }

    public void deleteMark(HttpServletRequest request, MarkDao markDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        Mark mark = new Mark();
        mark.setId(Integer.valueOf(request.getParameter("id")));

        markDao.delete(mark);

        listMark(request, markDao, response);
    }


    public void listStudent(HttpServletRequest request, StudentDao studentDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        request.setAttribute("students", studentDao.getAll());

        request.getRequestDispatcher(LIST_STUDENT).forward(request, response);
    }

    public void listSubject(HttpServletRequest request, SubjectDao subjectDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        request.setAttribute("subjects", subjectDao.getAll());

        request.getRequestDispatcher(LIST_SUBJECT).forward(request, response);
    }

    public void listMark(HttpServletRequest request, MarkDao markDao, HttpServletResponse response) throws ServletException, IOException, DaoException {

        request.setAttribute("marks", markDao.getAll());

        request.getRequestDispatcher(LIST_MARK).forward(request, response);
    }
}