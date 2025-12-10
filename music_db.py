# music_db.py

from typing import List, Tuple, Set
import mysql.connector  # or your DB-API driver


# -----------------------------
# Internal helper functions
# -----------------------------

def _get_or_create_artist(cur, name: str) -> int:
    cur.execute("SELECT artist_id FROM Artist WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Artist (name) VALUES (%s)", (name,))
    return cur.lastrowid


def _get_or_create_genre(cur, name: str) -> int:
    cur.execute("SELECT genre_id FROM Genre WHERE name = %s", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO Genre (name) VALUES (%s)", (name,))
    return cur.lastrowid


def _get_or_create_user(cur, username: str) -> int:
    cur.execute("SELECT user_id FROM `User` WHERE username = %s", (username,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO `User` (username) VALUES (%s)", (username,))
    return cur.lastrowid


def _get_song_id(cur, title: str, artist_name: str):
    """
    Find a song_id from (song title, artist name).
    Returns song_id or None if not found.
    """
    sql = """
        SELECT s.song_id
        FROM Song s
        JOIN Artist a ON s.artist_id = a.artist_id
        WHERE s.title = %s AND a.name = %s
    """
    cur.execute(sql, (title, artist_name))
    row = cur.fetchone()
    return row[0] if row else None


# -----------------------------
# Required functions
# -----------------------------

def clear_database(mydb) -> None:
    """
    Delete all rows from all tables in the database.
    Autograder will call this first.
    """
    cur = mydb.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in ["Rating", "SongGenre", "Song", "Album", "`User`", "Genre", "Artist"]:
            cur.execute(f"TRUNCATE TABLE {table}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        mydb.commit()
    finally:
        cur.close()


def load_single_songs(
    mydb,
    single_songs: List[Tuple[str, Tuple[str, ...], str, str]]
) -> Set[Tuple[str, str]]:
    """
    Load single (non-album) songs.

    each element of single_songs is:
        (song_title, (genre1, genre2, ...), artist_name, release_date_str)

    Returns:
        set of (artist_name, song_title) that could NOT be loaded
        (e.g., duplicate song for that artist).
    """
    bad: Set[Tuple[str, str]] = set()
    cur = mydb.cursor()
    try:
        for song_title, genres, artist_name, release_date in single_songs:
            # get/create artist
            artist_id = _get_or_create_artist(cur, artist_name)

            # check if this (artist, title) already exists
            cur.execute(
                """
                SELECT song_id FROM Song
                WHERE title = %s AND artist_id = %s
                """,
                (song_title, artist_id),
            )
            row = cur.fetchone()
            if row:
                # Duplicate according to the schema rule
                bad.add((artist_name, song_title))
                continue

            # Insert song as a single
            cur.execute(
                """
                INSERT INTO Song (title, artist_id, album_id, single_release_date)
                VALUES (%s, %s, NULL, %s)
                """,
                (song_title, artist_id, release_date),
            )
            song_id = cur.lastrowid

            # Attach genres
            for g in genres:
                genre_id = _get_or_create_genre(cur, g)
                # avoid duplicate (song_id, genre_id)
                cur.execute(
                    """
                    INSERT IGNORE INTO SongGenre (song_id, genre_id)
                    VALUES (%s, %s)
                    """,
                    (song_id, genre_id),
                )

        mydb.commit()
    finally:
        cur.close()

    return bad


def load_albums(
    mydb,
    albums: List[Tuple[str, str, str, List[str]]]
) -> Set[Tuple[str, str]]:
    """
    Load albums and their songs.

    each element of albums is:
        (album_title, artist_name, album_genre, [song_title_1, song_title_2, ...])

    Returns:
        set of (artist_name, album_title) that could NOT be loaded (e.g., duplicate album).
    """
    bad: Set[Tuple[str, str]] = set()
    cur = mydb.cursor()
    try:
        for album_title, artist_name, album_genre, song_titles in albums:
            artist_id = _get_or_create_artist(cur, artist_name)
            genre_id = _get_or_create_genre(cur, album_genre)

            # Check for duplicate album for that artist
            cur.execute(
                """
                SELECT album_id FROM Album
                WHERE artist_id = %s AND title = %s
                """,
                (artist_id, album_title),
            )
            row = cur.fetchone()
            if row:
                # Album already exists
                bad.add((artist_name, album_title))
                album_id = row[0]
            else:
                # Create album (release_date can be NULL or set elsewhere)
                cur.execute(
                    """
                    INSERT INTO Album (title, artist_id, release_date, genre_id)
                    VALUES (%s, %s, NULL, %s)
                    """,
                    (album_title, artist_id, genre_id),
                )
                album_id = cur.lastrowid

            # Create songs for this album
            for song_title in song_titles:
                # check if a song with same artist+title already exists
                cur.execute(
                    """
                    SELECT song_id FROM Song
                    WHERE title = %s AND artist_id = %s
                    """,
                    (song_title, artist_id),
                )
                row = cur.fetchone()
                if row:
                    # duplicate song title for this artist; skip, but record as bad
                    bad.add((artist_name, song_title))
                    continue

                cur.execute(
                    """
                    INSERT INTO Song (title, artist_id, album_id, single_release_date)
                    VALUES (%s, %s, %s, NULL)
                    """,
                    (song_title, artist_id, album_id),
                )
                song_id = cur.lastrowid

                # assign the album's genre to each song
                cur.execute(
                    """
                    INSERT IGNORE INTO SongGenre (song_id, genre_id)
                    VALUES (%s, %s)
                    """,
                    (song_id, genre_id),
                )

        mydb.commit()
    finally:
        cur.close()

    return bad


def load_users(mydb, users: List[str]) -> Set[str]:
    """
    Load users by username.

    Returns:
        set of usernames that could NOT be inserted (e.g., duplicates).
    """
    bad: Set[str] = set()
    cur = mydb.cursor()
    try:
        for username in users:
            cur.execute("SELECT user_id FROM `User` WHERE username = %s", (username,))
            row = cur.fetchone()
            if row:
                bad.add(username)
                continue
            cur.execute(
                "INSERT INTO `User` (username) VALUES (%s)",
                (username,),
            )
        mydb.commit()
    finally:
        cur.close()

    return bad


def load_song_ratings(
    mydb,
    song_ratings: List[Tuple[str, Tuple[str, str], int, str]]
) -> Set[Tuple[str, str, str]]:
    """
    Load song ratings.

    each element of song_ratings is:
        (username, (song_title, artist_name), rating_value, rating_date_str)

    Rating_value must be in [1..5].

    Returns:
        set of (username, song_title, artist_name) that could NOT be inserted
        (e.g., song/user missing or duplicate rating).
    """
    bad: Set[Tuple[str, str, str]] = set()
    cur = mydb.cursor()
    try:
        for username, (song_title, artist_name), rating_value, rating_date in song_ratings:
            # Check rating value
            if rating_value < 1 or rating_value > 5:
                bad.add((username, song_title, artist_name))
                continue

            # Ensure user exists
            user_id = _get_or_create_user(cur, username)

            # Find song
            song_id = _get_song_id(cur, song_title, artist_name)
            if song_id is None:
                bad.add((username, song_title, artist_name))
                continue

            # Check for existing rating from this user for this song
            cur.execute(
                """
                SELECT rating_id FROM Rating
                WHERE user_id = %s AND song_id = %s
                """,
                (user_id, song_id),
            )
            row = cur.fetchone()
            if row:
                bad.add((username, song_title, artist_name))
                continue

            cur.execute(
                """
                INSERT INTO Rating (user_id, song_id, rating_value, rating_date)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, song_id, rating_value, rating_date),
            )

        mydb.commit()
    finally:
        cur.close()

    return bad


# -----------------------------
# Query functions
# -----------------------------

def get_most_prolific_individual_artists(
    mydb,
    n: int,
    year_range: Tuple[int, int]
) -> List[Tuple[str, int]]:
    """
    Return top-n artists by number of songs released in the given [start_year, end_year].

    We count each song once. Release year is taken as:
        - YEAR(single_release_date) for singles
        - YEAR(Album.release_date) for album tracks (if non-NULL)
    """
    start_year, end_year = year_range
    cur = mydb.cursor()
    try:
        sql = """
            SELECT a.name,
                   COUNT(s.song_id) AS num_songs
            FROM Artist a
            JOIN Song s ON s.artist_id = a.artist_id
            LEFT JOIN Album al ON s.album_id = al.album_id
            WHERE (
                (s.single_release_date IS NOT NULL
                 AND YEAR(s.single_release_date) BETWEEN %s AND %s)
                OR
                (s.single_release_date IS NULL
                 AND al.release_date IS NOT NULL
                 AND YEAR(al.release_date) BETWEEN %s AND %s)
            )
            GROUP BY a.artist_id
            ORDER BY num_songs DESC, a.name ASC
            LIMIT %s
        """
        cur.execute(sql, (start_year, end_year, start_year, end_year, n))
        rows = cur.fetchall()
        return [(name, int(cnt)) for (name, cnt) in rows]
    finally:
        cur.close()


def get_artists_last_single_in_year(mydb, year: int) -> Set[str]:
    """
    Return the set of artist names whose LAST single (by date) falls in the given year.
    """
    cur = mydb.cursor()
    try:
        sql = """
            SELECT a.name
            FROM Artist a
            JOIN Song s
              ON s.artist_id = a.artist_id
             AND s.album_id IS NULL
             AND s.single_release_date IS NOT NULL
            GROUP BY a.artist_id
            HAVING YEAR(MAX(s.single_release_date)) = %s
        """
        cur.execute(sql, (year,))
        rows = cur.fetchall()
        return {name for (name,) in rows}
    finally:
        cur.close()


def get_top_song_genres(
    mydb,
    n: int
) -> List[Tuple[str, int]]:
    """
    Return the top-n genres by number of songs (a song counted once per genre).
    """
    cur = mydb.cursor()
    try:
        sql = """
            SELECT g.name,
                   COUNT(DISTINCT sg.song_id) AS cnt
            FROM Genre g
            JOIN SongGenre sg ON g.genre_id = sg.genre_id
            GROUP BY g.genre_id
            ORDER BY cnt DESC, g.name ASC
            LIMIT %s
        """
        cur.execute(sql, (n,))
        rows = cur.fetchall()
        return [(name, int(cnt)) for (name, cnt) in rows]
    finally:
        cur.close()


def get_album_and_single_artists(mydb) -> Set[str]:
    """
    Return the set of artist names who have BOTH:
        - at least one album track
        - at least one single
    """
    cur = mydb.cursor()
    try:
        sql = """
            SELECT DISTINCT a.name
            FROM Artist a
            JOIN Song s_single
              ON a.artist_id = s_single.artist_id
             AND s_single.album_id IS NULL
            JOIN Song s_album
              ON a.artist_id = s_album.artist_id
             AND s_album.album_id IS NOT NULL
        """
        cur.execute(sql)
        rows = cur.fetchall()
        return {name for (name,) in rows}
    finally:
        cur.close()


def get_most_rated_songs(
    mydb,
    year_range: Tuple[int, int],
    n: int
) -> List[Tuple[str, str, int]]:
    """
    Return the top-n songs by number of ratings in a given rating_date year range.

    Returns:
        list of (song_title, artist_name, rating_count)
        ordered by count DESC, then title ASC, then artist_name ASC.
    """
    start_year, end_year = year_range
    cur = mydb.cursor()
    try:
        sql = """
            SELECT s.title,
                   a.name,
                   COUNT(r.rating_id) AS cnt
            FROM Rating r
            JOIN Song s ON r.song_id = s.song_id
            JOIN Artist a ON s.artist_id = a.artist_id
            WHERE YEAR(r.rating_date) BETWEEN %s AND %s
            GROUP BY r.song_id
            ORDER BY cnt DESC, s.title ASC, a.name ASC
            LIMIT %s
        """
        cur.execute(sql, (start_year, end_year, n))
        rows = cur.fetchall()
        return [(title, artist_name, int(cnt)) for (title, artist_name, cnt) in rows]
    finally:
        cur.close()


def get_most_engaged_users(
    mydb,
    year_range: Tuple[int, int],
    n: int
) -> List[Tuple[str, int]]:
    """
    Return the top-n users by number of ratings they made in the given year range.

    Returns:
        list of (username, rating_count)
        ordered by count DESC, then username ASC.
    """
    start_year, end_year = year_range
    cur = mydb.cursor()
    try:
        sql = """
            SELECT u.username,
                   COUNT(r.rating_id) AS cnt
            FROM Rating r
            JOIN `User` u ON r.user_id = u.user_id
            WHERE YEAR(r.rating_date) BETWEEN %s AND %s
            GROUP BY u.user_id
            ORDER BY cnt DESC, u.username ASC
            LIMIT %s
        """
        cur.execute(sql, (start_year, end_year, n))
        rows = cur.fetchall()
        return [(username, int(cnt)) for (username, cnt) in rows]
    finally:
        cur.close()
