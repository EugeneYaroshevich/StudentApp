<%@ page import="dto.Subject" %>
<%@ page import="java.util.List" %>
<%@ page isErrorPage="true" language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<html>
<head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <title>List Subject</title>
</head>
<body bgcolor="#FBF9F9">

<div align="center" style="padding: 5px;">
 
   <jsp:include page="header.jsp"></jsp:include>
   <jsp:include page="menu.jsp"/>
   
    <%
List<Subject> subjects = (List<Subject>) request.getAttribute("subjects");
    %>
        <p></p>
		
        <table border=1 width=500 cellpadding=1 cellspacing=1 bgcolor="#EBF1F4">
		
		<tr>
            <th colspan="5" bgcolor="#BADFF8">List Subjects</th>
        </tr>
            <tr>
                <th>Id</th>
                <th>Name</th>
                <th colspan=2>Action</th>
            </tr>
			<tr>
            <%
            for (Subject subject : subjects) {
            %>
                <th><%=subject.getId()%></th>
                <td><%=subject.getName().toLowerCase()%></td>
                <th><a href="StudentServlet?action=editSubject&id=<%=subject.getId()%>">Edit</a></th>
                <th><a href="StudentServlet?action=deleteSubject&id=<%=subject.getId()%>">Delete</a></th>
            </tr>
            <%
            }
            %>
        </table>
</div>   
        
		<jsp:include page="footer.jsp"></jsp:include>
		
</body>
</html>