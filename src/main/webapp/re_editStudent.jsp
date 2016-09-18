<%@ page import="java.util.HashMap" %>
<%@ page isErrorPage="true" language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<html>
<head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <title>Edit Student</title>
</head>
<body bgcolor="#FBF9F9">

<div align="center" style="padding: 5px;">
 
   <jsp:include page="header.jsp"></jsp:include>
   <jsp:include page="menu.jsp"/>
 
	<%
	HashMap<String,String> errors = (HashMap<String, String>) request.getAttribute("errors");
	%> 
   
   <form method="post" action="StudentServlet">
    <input type="hidden" name="action" value="saveEditStudent"/>
	
    <p></p>
	
    <table border=1 width=500 cellpadding=1 cellspacing=1 bgcolor="#EBF1F4">
	
	    <tr>
            <th colspan="2" bgcolor="#BADFF8">Edit Student</th>
        </tr>
        <tr>
            <td width="10%">Student ID:</td>
            <td width="10%"><input type="text" readonly="readonly" name="id" size="60" value="<%=request.getAttribute("id")%>"/></td>
        </tr>
        <tr>
            <td width="10%">Name:</td>
            <td width="10%"><input type="text" name="name" size="60" value="<%=request.getAttribute("name")%>"/><span style="color: red;"><%=errors.get("nameStudent")%></span></td>
        </tr>
        <tr>
            <td width="10%">Surname:</td>
            <td width="10%"><input type="text" name="surname" size="60" value="<%=request.getAttribute("surname")%>"/><span style="color: red;"><%=errors.get("surnameStudent")%></span></td>
        </tr>

        <tr>
            <th colspan="2"><input type="submit" value="Edit & Save"/><input type="reset" value="Reset"/></th>
        </tr>
    </table>
</form>
   
</div>   

<jsp:include page="footer.jsp"></jsp:include>

</body>
</html>