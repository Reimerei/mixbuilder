package models;

import play.data.validation.Constraints;
import play.db.ebean.Model;

import javax.persistence.Entity;


@Entity
public class Track extends Model
{
    @Constraints.Required
    public String artist;

    @Constraints.Required
    public String trackName;

    public String trackLength;
}