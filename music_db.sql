
CREATE DATABASE IF NOT EXISTS sab541_music_db;
USE sab541_music_db;

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS Rating;
DROP TABLE IF EXISTS SongGenre;
DROP TABLE IF EXISTS Song;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS `User`;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Artist;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE Artist (
    artist_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name      VARCHAR(200) NOT NULL UNIQUE
) ;

CREATE TABLE Genre (
    genre_id SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name     VARCHAR(80) NOT NULL UNIQUE
) ;

CREATE TABLE `User` (
    user_id  INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE
) ;

CREATE TABLE Album (
    album_id     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    title        VARCHAR(200) NOT NULL,
    artist_id    INT UNSIGNED NOT NULL,
    release_date DATE,
    genre_id     SMALLINT UNSIGNED,

    UNIQUE KEY uq_album_artist_title (artist_id, title),

    CONSTRAINT fk_album_artist
        FOREIGN KEY (artist_id)
        REFERENCES Artist(artist_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT fk_album_genre
        FOREIGN KEY (genre_id)
        REFERENCES Genre(genre_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
) ;


CREATE TABLE Song (
    song_id             INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    title               VARCHAR(200) NOT NULL,
    artist_id           INT UNSIGNED NOT NULL,
    album_id            INT UNSIGNED NULL,
    single_release_date DATE NULL,

    UNIQUE KEY uq_song_artist_title (artist_id, title),

    CONSTRAINT fk_song_artist
        FOREIGN KEY (artist_id)
        REFERENCES Artist(artist_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT fk_song_album
        FOREIGN KEY (album_id)
        REFERENCES Album(album_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
) ;

-- SongGenre: many-to-many between Song and Genre
CREATE TABLE SongGenre (
    song_id  INT UNSIGNED NOT NULL,
    genre_id SMALLINT UNSIGNED NOT NULL,

    PRIMARY KEY (song_id, genre_id),

    CONSTRAINT fk_songgenre_song
        FOREIGN KEY (song_id)
        REFERENCES Song(song_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT fk_songgenre_genre
        FOREIGN KEY (genre_id)
        REFERENCES Genre(genre_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ;

-- Rating: one rating per (user, song)
CREATE TABLE Rating (
    rating_id    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id      INT UNSIGNED NOT NULL,
    song_id      INT UNSIGNED NOT NULL,
    rating_value TINYINT UNSIGNED NOT NULL,
    rating_date  DATE NOT NULL,

    UNIQUE KEY uq_user_song (user_id, song_id),

    CONSTRAINT fk_rating_user
        FOREIGN KEY (user_id)
        REFERENCES `User`(user_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT fk_rating_song
        FOREIGN KEY (song_id)
        REFERENCES Song(song_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CONSTRAINT chk_rating_value
        CHECK (rating_value BETWEEN 1 AND 5)
) ;
