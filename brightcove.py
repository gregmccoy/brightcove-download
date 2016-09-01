import requests
import psycopg2
import base64
import configparser

config = configparser.ConfigParser()


def readConf(option):
    config.read("brightcove.conf")
    value = config['DEFAULT'][option]
    return(value)

def get_videos(page):
    result = requests.get(readConf("ENDPOINT"), params=dict(
        command="search_videos",
        token=readConf("TOKEN"),
        media_delivery="http",
        page_number=page,
    ))
    return result.json()

def get_playlist():
    result = requests.get(readConf("ENDPOINT"), params=dict(
        command="find_all_playlists",
        token=readConf("TOKEN"),
    ))
    return result.json()

def parse_playlist(playlist, cur):
    for item in playlist['items']:
        print("Copying playlist - " + str(item['name']))
        videos = ""
        for video in item['videos']:
            videos += str(video["id"]) + "|"
        cur.execute("INSERT INTO Playlist (brightcoveID, Name, shortDesc, videos) VALUES (%s, %s, %s, %s);" \
                , (str(item["id"]), str(item["name"]), str(item["shortDescription"]), videos))

    return str(len(playlist['items']))

def parse_videos(videos, cur):
    for item in videos['items']:
        tags = ""
        for tag in item['tags']:
            tags += tag + "|"

        print("Downloading - " + str(item["name"]))
        r = requests.get(item["FLVURL"], stream=True)
        with open(readConf("PATH") + str(item['id']) + ".mp4", 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        cur.execute("INSERT INTO Videos (brightcoveID, Name, PosterFrame, longDesc, shortDesc, tags, linkText) \
                VALUES (%s, %s, %s, %s, %s, %s, %s);", (str(item['id']), str(item['name']), \
                str(item['videoStillURL']), str(item['longDescription']), str(item['shortDescription']), \
                tags, str(item['linkText'])))

    return str(len(videos['items']))


def process():
    try:
        conn=psycopg2.connect("dbname={} user={} password={}".format(readConf("DBNAME"), readConf("USER"), readConf("PASSWORD")))
        conn.autocommit = True
    except:
        print("Database connection failed")
        return

    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE Videos;")
        cur.execute("DROP TABLE Playlist;")
    except:
        print("Failed to drop table ")

    try:
        cur.execute("CREATE TABLE Videos(Id SERIAL PRIMARY KEY, brightcoveID TEXT,  Name VARCHAR(200), PosterFrame TEXT, longDesc TEXT, shortDesc TEXT, tags TEXT, linkText TEXT);")
        cur.execute("CREATE TABLE Playlist(Id SERIAL PRIMARY KEY, brightcoveID TEXT,  Name VARCHAR(200), shortDesc TEXT, videos TEXT);")
    except:
        print("Failed to create table ")
        return

    data = get_playlist()
    count = parse_playlist(data, cur)
    print(count + " playlists")

    count = 1
    page = 0
    while count != "0":
        data = get_videos(page)
        count = parse_videos(data, cur)
        print(count + " videos")
        print ("Page " + str(page))
        page += 1


process()
