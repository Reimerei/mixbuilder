package controllers;

import play.*;
import play.db.DB;
import play.mvc.*;

import views.html.*;

import java.sql.Connection;

public class Application extends Controller {

    public static Result index() {

        Connection c = DB.getConnection("mixbuilder");

        return ok(index.render("D"));
    }

}
