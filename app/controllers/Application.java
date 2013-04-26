package controllers;

import models.Track;
import play.*;
import play.db.DB;
import play.db.ebean.Model;
import play.mvc.*;

import views.html.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Application extends Controller {

    public static Result index() throws SQLException
    {
        Track t = new Track();

        t.artist = "boss";
        t.trackName = "song";

        t.save();

        List<Track> ts = Track.find.where().ilike("artist","boss").findList();


        return ok( index.render(ts.get(0).trackName));
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
