package controllers;

import play.*;
import play.db.DB;
import play.mvc.*;

import views.html.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Application extends Controller {

    public static Result index() throws SQLException {

        return ok( index.render("X"));
    }

    public static Result showColumn(String columnNr) throws SQLException
    {
        Connection con = DB.getConnection("mixbuilder");

        Statement st= con.createStatement();
        ResultSet rs= st.executeQuery("SELECT * from tunes");

        ArrayList<String> column = new ArrayList<String>();

        while(rs.next())
        {
            column.add(rs.getString(Integer.parseInt(columnNr)));
        }

        return ok(showColumn.render(column));
    }
}
