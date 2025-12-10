# hw3_tester.py
# Local tester for the tricky cases from HW3

from music_db import (
    get_connection,
    clear_database,
    load_single_songs,
    load_albums,
    load_users,
    load_song_ratings,
    get_top_song_genres,
    get_most_rated_songs,
    get_most_engaged_users,
    get_album_and_single_artists,
)


def print_header(title: str):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ---------------------------------------------------------
# 1) load_single_songs – duplicate handling
# ---------------------------------------------------------

def test_load_single_songs_duplicate(mydb):
    print_header("TEST: load_single_songs – duplicate song for same artist")

    clear_database(mydb)

    # First load: should succeed, no rejects
    singles1 = [
        ("Same Song", ("Rock",), "Artist A", "2020-01-01"),
    ]
    bad1 = load_single_songs(mydb, singles1)
    print("First load bad set:", bad1)
    assert bad1 == set(), "First single load should have no rejects"

    # Second load: same artist + same title -> should be rejected
    singles2 = [
        ("Same Song", ("Rock",), "Artist A", "2020-02-02"),
    ]
    bad2 = load_single_songs(mydb, singles2)
    print("Second load bad set:", bad2)

    expected = {("Artist A", "Same Song")}
    print("Expected rejects:", expected)
    assert bad2 == expected, "Duplicate single should be rejected exactly once"

    print("✅ load_single_songs duplicate test passed.")


# ---------------------------------------------------------
# 2) load_albums – 5-tuple format + album rejections
# ---------------------------------------------------------

def test_load_albums_basic_and_duplicates(mydb):
    print_header("TEST: load_albums – basic load + duplicate album")

    clear_database(mydb)

    # Album tuple format:
    # (album_title, artist_name, release_date, album_genre, [song titles])

    albums1 = [
        ("Album One", "Artist A", "2020-05-01", "Rock", ["Track 1", "Track 2"]),
    ]
    bad1 = load_albums(mydb, albums1)
    print("First load bad set:", bad1)
    assert bad1 == set(), "First album load should have no rejects"

    # Duplicate album: same (artist, title)
    albums2 = [
        ("Album One", "Artist A", "2021-01-01", "Rock", ["Other 1"]),
    ]
    bad2 = load_albums(mydb, albums2)
    print("Second load bad set:", bad2)
    expected = {("Artist A", "Album One")}
    print("Expected rejects:", expected)
    assert bad2 == expected, "Duplicate album by same artist should be rejected"

    print("✅ load_albums basic + duplicate album test passed.")


def test_load_albums_song_duplicates(mydb):
    print_header("TEST: load_albums – album with song that duplicates existing song")

    clear_database(mydb)

    # 1) Load a single for Artist A called "Hit"
    singles = [
        ("Hit", ("Rock",), "Artist A", "2020-01-01"),
    ]
    bad_singles = load_single_songs(mydb, singles)
    assert bad_singles == set()

    # 2) Try to load an album for Artist A that includes "Hit"
    albums = [
        ("Problem Album", "Artist A", "2021-01-01", "Rock", ["Hit", "Other"]),
    ]
    bad_albums = load_albums(mydb, albums)
    print("Album with duplicate song bad set:", bad_albums)

    expected = {("Artist A", "Problem Album")}
    print("Expected rejects:", expected)
    assert bad_albums == expected, (
        "Album containing song that already exists for artist "
        "should reject whole album"
    )

    print("✅ load_albums song-duplicate test passed.")


def test_load_albums_song_duplicates_between_albums(mydb):
    print_header("TEST: load_albums – duplicate song across two albums for same artist")

    clear_database(mydb)

    # First album for Artist B, with song "Shared"
    albums1 = [
        ("First Album", "Artist B", "2020-01-01", "Jazz", ["Shared", "Unique 1"]),
    ]
    bad1 = load_albums(mydb, albums1)
    assert bad1 == set()

    # Second album for Artist B, also containing "Shared" -> should be rejected
    albums2 = [
        ("Second Album", "Artist B", "2021-01-01", "Jazz", ["Shared", "Unique 2"]),
    ]
    bad2 = load_albums(mydb, albums2)
    print("Second album bad set:", bad2)

    expected = {("Artist B", "Second Album")}
    print("Expected rejects:", expected)
    assert bad2 == expected, (
        "Second album with duplicate song title by same artist "
        "should be rejected"
    )

    print("✅ load_albums cross-album song-duplicate test passed.")


# ---------------------------------------------------------
# 3) load_song_ratings – valid vs rejects
# ---------------------------------------------------------

def test_load_song_ratings(mydb):
    print_header("TEST: load_song_ratings – valid and invalid ratings")

    clear_database(mydb)

    # Setup: one single, one album song, and two users
    singles = [
        ("Single X", ("Rock",), "Artist A", "2020-01-01"),
    ]
    load_single_songs(mydb, singles)

    albums = [
        ("Album One", "Artist A", "2020-02-01", "Rock", ["Track 1"]),
    ]
    load_albums(mydb, albums)

    users = ["alice", "bob"]
    load_users(mydb, users)

    # 1) Valid rating: alice rates Single X
    r1 = [
        ("alice", ("Single X", "Artist A"), 5, "2021-01-01"),
    ]
    bad1 = load_song_ratings(mydb, r1)
    print("Valid rating bad set:", bad1)
    assert bad1 == set(), "Valid rating should not be rejected"

    # 2) Duplicate rating: alice rates Single X again
    r2 = [
        ("alice", ("Single X", "Artist A"), 4, "2021-02-02"),
    ]
    bad2 = load_song_ratings(mydb, r2)
    print("Duplicate rating bad set:", bad2)
    expected2 = {("alice", "Single X", "Artist A")}
    print("Expected rejects:", expected2)
    assert bad2 == expected2, "Duplicate rating should be rejected"

    # 3) Rater not in database
    r3 = [
        ("charlie", ("Single X", "Artist A"), 3, "2021-03-03"),
    ]
    bad3 = load_song_ratings(mydb, r3)
    print("Missing user bad set:", bad3)
    expected3 = {("charlie", "Single X", "Artist A")}
    assert bad3 == expected3

    # 4) Song not in database
    r4 = [
        ("alice", ("Nonexistent Song", "Artist A"), 4, "2021-04-04"),
    ]
    bad4 = load_song_ratings(mydb, r4)
    print("Missing song bad set:", bad4)
    expected4 = {("alice", "Nonexistent Song", "Artist A")}
    assert bad4 == expected4

    # 5) Out-of-bounds rating
    r5 = [
        ("alice", ("Single X", "Artist A"), 0, "2021-05-05"),
    ]
    bad5 = load_song_ratings(mydb, r5)
    print("Out-of-bounds rating bad set:", bad5)
    expected5 = {("alice", "Single X", "Artist A")}
    assert bad5 == expected5

    print("✅ load_song_ratings tests passed.")


# ---------------------------------------------------------
# 4) get_top_song_genres / most_rated / most_engaged / album+single
# ---------------------------------------------------------

def setup_for_query_tests(mydb):
    """Populate DB with a small, known dataset for query testing."""
    clear_database(mydb)

    # Singles
    singles = [
        ("Rock Single", ("Rock",), "Artist A", "2020-01-01"),
        ("Pop Single", ("Pop",), "Artist B", "2020-02-01"),
        ("Dual Genre", ("Rock", "Jazz"), "Artist C", "2021-03-01"),
    ]
    load_single_songs(mydb, singles)

    # Albums
    albums = [
        ("Rock Album", "Artist A", "2020-04-01", "Rock", ["Album Rock 1", "Album Rock 2"]),
        ("Jazz Album", "Artist C", "2021-05-01", "Jazz", ["Album Jazz 1"]),
    ]
    load_albums(mydb, albums)

    # Users
    users = ["alice", "bob", "carol"]
    load_users(mydb, users)

    # Ratings:
    # Rock Single: 2 ratings
    # Pop Single: 1 rating
    # Dual Genre: 3 ratings
    # Album Rock 1: 1 rating
    # Album Jazz 1: 1 rating
    ratings = [
        ("alice", ("Rock Single", "Artist A"), 5, "2021-01-01"),
        ("bob",   ("Rock Single", "Artist A"), 4, "2021-01-02"),

        ("alice", ("Pop Single", "Artist B"), 3, "2021-02-01"),

        ("alice", ("Dual Genre", "Artist C"), 4, "2021-03-01"),
        ("bob",   ("Dual Genre", "Artist C"), 5, "2021-03-02"),
        ("carol", ("Dual Genre", "Artist C"), 5, "2021-03-03"),

        ("carol", ("Album Rock 1", "Artist A"), 4, "2021-04-01"),
        ("bob",   ("Album Jazz 1", "Artist C"), 4, "2021-05-01"),
    ]
    load_song_ratings(mydb, ratings)


def test_get_top_song_genres(mydb):
    print_header("TEST: get_top_song_genres")

    setup_for_query_tests(mydb)

    res = get_top_song_genres(mydb, 10)
    print("get_top_song_genres(10) =", res)

    # Rough expectations:
    # Songs & their genres:
    # Rock Single (Rock)
    # Pop Single (Pop)
    # Dual Genre (Rock, Jazz)
    # Album Rock 1 (Rock)
    # Album Rock 2 (Rock)
    # Album Jazz 1 (Jazz)
    #
    # Counts:
    # Rock: 4 songs
    # Jazz: 2 songs
    # Pop: 1 song
    expected_first = ("Rock", 4)
    assert res[0] == expected_first, "Rock should be top genre with count 4"

    print("✅ get_top_song_genres test passed.")


def test_album_and_single_artists(mydb):
    print_header("TEST: get_album_and_single_artists")

    setup_for_query_tests(mydb)

    res = get_album_and_single_artists(mydb)
    print("get_album_and_single_artists() =", res)

    # In setup_for_query_tests:
    # Artist A: has singles + album tracks  -> should appear
    # Artist B: only single
    # Artist C: single + album tracks       -> should appear
    assert "Artist A" in res
    assert "Artist C" in res
    assert "Artist B" not in res

    print("✅ get_album_and_single_artists test passed.")


def test_get_most_rated_songs(mydb):
    print_header("TEST: get_most_rated_songs")

    setup_for_query_tests(mydb)

    res = get_most_rated_songs(mydb, (2021, 2021), 10)
    print("get_most_rated_songs((2021,2021),10) =", res)

    # Dual Genre has 3 ratings, Rock Single has 2, others have 1
    first = res[0]
    expected_title = "Dual Genre"
    assert first[0] == expected_title and first[2] == 3, (
        "Dual Genre should be the most rated song with count 3"
    )

    print("✅ get_most_rated_songs test passed.")


def test_get_most_engaged_users(mydb):
    print_header("TEST: get_most_engaged_users")

    setup_for_query_tests(mydb)

    res = get_most_engaged_users(mydb, (2021, 2021), 10)
    print("get_most_engaged_users((2021,2021),10) =", res)

    # From setup:
    # alice: 3 ratings (Rock Single, Pop Single, Dual Genre)
    # bob:   3 ratings (Rock Single, Dual Genre, Album Jazz 1)
    # carol: 2 ratings (Dual Genre, Album Rock 1)
    # Ties broken alphabetically by username
    expected_first = ("alice", 3)
    expected_second = ("bob", 3)

    assert res[0] == expected_first
    assert res[1] == expected_second

    print("✅ get_most_engaged_users test passed.")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":
    mydb = get_connection()

    try:
        test_load_single_songs_duplicate(mydb)
        test_load_albums_basic_and_duplicates(mydb)
        test_load_albums_song_duplicates(mydb)
        test_load_albums_song_duplicates_between_albums(mydb)
        test_load_song_ratings(mydb)
        test_get_top_song_genres(mydb)
        test_album_and_single_artists(mydb)
        test_get_most_rated_songs(mydb)
        test_get_most_engaged_users(mydb)
    finally:
        mydb.close()
