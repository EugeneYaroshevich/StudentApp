package mysql;

import dto.Student;
import exception.DaoException;
import org.dbunit.*;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.*;


public class StudentDaoTest_TestNG extends DBTestCase {

    public StudentDaoTest_TestNG() throws DaoException {
        setProperty();
    }

    private void setProperty() throws DaoException {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("test_mysql.properties")) {

            properties.load(in);

            System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS, properties.getProperty("driver"));
            System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL, properties.getProperty("url"));
            System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME, properties.getProperty("user"));
            System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD, properties.getProperty("password"));
            System.setProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_SCHEMA, "");
        } catch (Exception e) {
            throw new DaoException("Can not find properties file ", e);
        }
    }

    @Override
    protected IDataSet getDataSet() throws Exception {
        return new FlatXmlDataSetBuilder().build(new File("src/test/resources/all_tables_original_dataset.xml"));
    }


    @BeforeMethod
    public void setUp() throws Exception {
        IDatabaseTester databaseTester = new JdbcDatabaseTester(System.getProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_DRIVER_CLASS),
                System.getProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_CONNECTION_URL),
                System.getProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_USERNAME),
                System.getProperty(PropertiesBasedJdbcDatabaseTester.DBUNIT_PASSWORD));
        databaseTester.setSetUpOperation(DatabaseOperation.CLEAN_INSERT);
        databaseTester.setTearDownOperation(DatabaseOperation.NONE);
        IDataSet expectedDataSet = getDataSet();
        databaseTester.setDataSet(expectedDataSet);
        databaseTester.onSetup();
    }


    @Test(groups = "update")
    public void testUpdate() throws Exception {

        StudentDao studentDao = new StudentDao(getDatabaseTester().getConnection().getConnection());
        Student student = new Student();
        student.setId(10);
        student.setName("VLAD");
        student.setSurname("VISOCKIY");
        studentDao.update(student);

        QueryDataSet databaseDataset = new QueryDataSet(getDatabaseTester().getConnection());
        databaseDataset.addTable("student");
        IDataSet updateDataset = new FlatXmlDataSetBuilder().build(new File("src/test/resources/student_data_update.xml"));

        String[] ignore = {"id"};
        Assertion.assertEqualsIgnoreCols(updateDataset, databaseDataset, "student", ignore);
    }

    @Test(dependsOnGroups = "get")
    public void testAdd() throws Exception {
        StudentDao studentDao = new StudentDao(getDatabaseTester().getConnection().getConnection());
        Student student = new Student();
        student.setName("MARINA");
        student.setSurname("MARINOVA");
        studentDao.add(student);

        QueryDataSet databaseDataset = new QueryDataSet(getDatabaseTester().getConnection());
        databaseDataset.addTable("student");
        IDataSet updateDataset = new FlatXmlDataSetBuilder().build(new File("src/test/resources/student_data_add.xml"));

        String[] ignore = {"id"};
        Assertion.assertEqualsIgnoreCols(updateDataset, databaseDataset, "student", ignore);
    }

    @Test(groups = "update", dependsOnMethods = "testUpdate")
    public void testDelete() throws Exception {

        StudentDao studentDao = new StudentDao(getDatabaseTester().getConnection().getConnection());
        Student student = new Student();
        student.setId(10);
        studentDao.delete(student);

        QueryDataSet databaseDataset = new QueryDataSet(getDatabaseTester().getConnection());
        databaseDataset.addTable("student");
        IDataSet deleteDataset = new FlatXmlDataSetBuilder().build(new File("src/test/resources/student_data_delete.xml"));
        String[] ignore = {"id"};
        Assertion.assertEqualsIgnoreCols(deleteDataset, databaseDataset, "student", ignore);
    }

    @Test(groups = "get")
    public void testGetAll() throws Exception {
        StudentDao studentDao = new StudentDao(getDatabaseTester().getConnection().getConnection());
        List<Student> studentList = studentDao.getAll();
        assertEquals("The number of the received records doesn't correspond to what was expected: ",
                getDataSet().getTable("student").getRowCount(), studentList.size());
    }

    @Test(groups = "get")
    public void testGetById() throws Exception {
        StudentDao studentDao = new StudentDao(getDatabaseTester().getConnection().getConnection());
        Student student = studentDao.getById(1);

        assertEquals("The received ID of student doesn't correspond to what was expected: ", Integer.valueOf(1), student.getId());
        assertEquals("The received NAME of student doesn't correspond to what was expected: ", "ALEXANDER", student.getName());
        assertEquals("The received SURNAME of student doesn't correspond to what was expected: ", "MAKEDONSKIY", student.getSurname());
    }


}