<%@ page isErrorPage="true" language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<html>
<head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8">
    <title>Menu</title>
</head>
<body bgcolor="#FBF9F9">

<div align="center" style="padding: 20px;">
 
 <p></p>
 
    <table border=1 width=500 cellpadding=1 cellspacing=1 bgcolor="#EBF1F4">
	    <tr>
            <th colspan="3" bgcolor="#BADFF8">Menu</th>
        </tr>
        <tr align="center">
            <th><a href="StudentServlet?action=student">Add Student</a></th>
            <th><a href="StudentServlet?action=subject">Add Subject</a></th>
			<th><a href="StudentServlet?action=mark">Add Mark</a></th>
        </tr>
        <tr align="center">
            <th><a href="StudentServlet?action=listStudent">List Students</a></th>
            <th><a href="StudentServlet?action=listSubject">List Subjects</a></th>
			<th><a href="StudentServlet?action=listMark">List Marks</a></th>
    </table>
   
</div>   

   
</body>
</html>