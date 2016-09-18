<%@ page import="dto.Student" %>
<%@ page import="java.util.List" %>
<%@ page isErrorPage="true" language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<html>
<head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <title>List Student</title>
</head>
<body bgcolor="#FBF9F9">

<div align="center" style="padding: 5px;">
 
   <jsp:include page="header.jsp"></jsp:include>
   <jsp:include page="menu.jsp"/>
   
   
    <%
List<Student> students = (List<Student>) request.getAttribute("students");
    %>
        <p></p>
		
        <table border=1 width=500 cellpadding=1 cellspacing=1 bgcolor="#EBF1F4">
		
		<tr>
            <th colspan="5" bgcolor="#BADFF8">List Students</th>
        </tr>
		
            <tr>
                <th>Id</th>
                <th>Name</th>
                <th>Surname</th>
                <th colspan=2>Action</th>
            </tr>
			<tr>
            <%
            for (Student student : students) {
            %>
                <th><%=student.getId()%></th>
                <td><%=student.getName().toLowerCase()%></td>
                <td><%=student.getSurname().toLowerCase()%></td>
                <th><a href="StudentServlet?action=editStudent&id=<%=student.getId()%>">Edit</a></th>
                <th><a href="StudentServlet?action=deleteStudent&id=<%=student.getId()%>">Delete</a></th>
            </tr>
            <%
            }
            %>
        </table>
   
</div>   

<jsp:include page="footer.jsp"></jsp:include>

</body>
</html>