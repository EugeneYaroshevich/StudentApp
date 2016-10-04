package mysql;

import java.io.*;
import java.util.List;

import dto.Student;
import org.dbunit.Assertion;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.*;

import static org.junit.Assert.*;


public class StudentDaoTest {

    private static IDatabaseTester databaseTester;
    private IDataSet expectedDataSet;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

        databaseTester = new JdbcDatabaseTester("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test_daostudentdb?useUnicode=true&characterSet=UTF-8&useSSL=false", "root", "root");
        databaseTester.setSetUpOperation(DatabaseOperation.CLEAN_INSERT);
        databaseTester.setTearDownOperation(DatabaseOperation.NONE);
    }

    @Before
    public void setUp() throws Exception {

        expectedDataSet = new FlatXmlDataSetBuilder().build(new File("src/test/resources/all_tables_original_dataset.xml"));
        databaseTester.setDataSet(expectedDataSet);
        databaseTester.onSetup();
    }

    @After
    public void tearDown() throws Exception {
        databaseTester.onTearDown();
    }

    @Test
    public void testUpdate() throws Exception {

        StudentDao studentDao = new StudentDao(databaseTester.getConnection().getConnection());
        Student student = new Student();
        student.setId(10);
        student.setName("VLAD");
        student.setSurname("VISOCKIY");
        studentDao.update(student);

        QueryDataSet databaseDataset = new QueryDataSet(databaseTester.getConnection());
        databaseDataset.addTable("student");
        IDataSet updateDataset = new FlatXmlDataSetBuilder().build(new File("src/test/resources/student_data_update.xml"));

        String[] ignore = {"id"};
        Assertion.assertEqualsIgnoreCols(updateDataset, databaseDataset, "student", ignore);
    }

    @Test
    public void testAdd() throws Exception {
        StudentDao studentDao = new StudentDao(databaseTester.getConnection().getConnection());
        Student student = new Student();
        student.setName("MARINA");
        student.setSurname("MARINOVA");
        studentDao.add(student);

        QueryDataSet databaseDataset = new QueryDataSet(databaseTester.getConnection());
        databaseDataset.addTable("student");
        IDataSet updateDataset = new FlatXmlDataSetBuilder().build(new File("src/test/resources/student_data_add.xml"));

        String[] ignore = {"id"};
        Assertion.assertEqualsIgnoreCols(updateDataset, databaseDataset, "student", ignore);
    }

    @Test
    public void testDelete() throws Exception {

        StudentDao studentDao = new StudentDao(databaseTester.getConnection().getConnection());
        Student student = new Student();
        student.setId(10);
        studentDao.delete(student);

        QueryDataSet databaseDataset = new QueryDataSet(databaseTester.getConnection());
        databaseDataset.addTable("student");
        IDataSet deleteDataset = new FlatXmlDataSetBuilder().build(new File("src/test/resources/student_data_delete.xml"));
        String[] ignore = {"id"};
        Assertion.assertEqualsIgnoreCols(deleteDataset, databaseDataset, "student", ignore);
    }

    @Test
    public void testGetAll() throws Exception {
        StudentDao studentDao = new StudentDao(databaseTester.getConnection().getConnection());
        List<Student> studentList = studentDao.getAll();
        assertEquals(studentList.size(), expectedDataSet.getTable("student").getRowCount());
    }

    @Test
    public void testGetById() throws Exception {
        StudentDao studentDao = new StudentDao(databaseTester.getConnection().getConnection());
        Student student = studentDao.getById(1);

        assertEquals(student.getId(), Integer.valueOf(1));
        assertEquals(student.getName(), "ALEXANDER");
        assertEquals(student.getSurname(), "MAKEDONSKIY");
    }

}

//        IDataSet databaseDataset = databaseTester.getConnection().createDataSet();
//        ITable databaseTable = databaseDataset.getTable("student");
//        Assertion.assertEquals(databaseTable, expectedDataSet.getTable("student"));

//        String[] ignore = {"id"};
//        Assertion.assertEqualsIgnoreCols(deleteDataset, databaseDataset, "student", ignore);


// загружаем набор с тестовыми данными
//        IDataSet expectedDataSet = new FlatXmlDataSet(new InputStreamReader(new FileInputStream("src/test/resources/all-tables-dataset.xml"), "utf-8"));
//        IDataSet expectedDataSet = new FlatXmlDataSet(getClass().getResourceAsStream("src/test/resources/all-tables-dataset.xml"));
//        IDataSet expectedDataSet = new FlatXmlDataSetBuilder().build(
//                Thread.currentThread().getContextClassLoader()
//                        .getResourceAsStream("src/test/resources/all-tables-dataset.xml"));