package models;

import play.data.validation.Constraints;
import play.db.ebean.Model;

import javax.persistence.Entity;
import javax.persistence.Id;


@Entity
public class Track extends Model
{
    @Id
    public Long id;

    @Constraints.Required
    public String artist;

    @Constraints.Required
    public String trackName;

    public String trackLength;

    public static Finder<Long,Track> find = new Finder<Long,Track>(Long.class, Track.class);
}