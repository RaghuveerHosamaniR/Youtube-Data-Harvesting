# ----------------------------
# YOUTUBE DATA HARVESTING APP
# ----------------------------

import streamlit as st
import pandas as pd
import pymongo
import psycopg2
from googleapiclient.discovery import build

# ----------------------------
# YOUTUBE API CONNECTION
# ----------------------------
def Api_connect():
    Api_Id = "****************"
    youtube = build("youtube", "v3", developerKey=Api_Id)
    return youtube

youtube = Api_connect()

# ----------------------------
# FETCH CHANNEL INFORMATION
# ----------------------------
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(
            Channel_Name=i["snippet"]["title"],
            Channel_Id=i["id"],
            Subscribers=i['statistics']['subscriberCount'],
            Views=i["statistics"]["viewCount"],
            Total_Videos=i["statistics"]["videoCount"],
            Channel_Description=i["snippet"]["description"],
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
    return data

# ----------------------------
# GET VIDEO IDS
# ----------------------------
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response1['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# ----------------------------
# GET VIDEO INFORMATION
# ----------------------------
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            data = dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics'].get('favoriteCount'),
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data

# ----------------------------
# GET COMMENTS INFORMATION
# ----------------------------
def get_comment_info(video_ids, max_comments=50):
    comment_data = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_comments
            )
            response = request.execute()
            for item in response.get('items', []):
                data = dict(
                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
        except:
            continue
    return comment_data

# ----------------------------
# GET PLAYLIST DETAILS
# ----------------------------
def get_playlist_details(channel_id):
    all_data = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount']
            )
            all_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_data

# ----------------------------
# MONGODB CONNECTION
# ----------------------------
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Youtube_data"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    # Insert separately to avoid large document
    db["channels"].insert_one(ch_details)
    if pl_details: db["playlists"].insert_many(pl_details)
    if vi_details: db["videos"].insert_many(vi_details)
    if com_details: db["comments"].insert_many(com_details)

    return "Upload completed successfully"

# ----------------------------
# POSTGRESQL QUERY FUNCTION
# ----------------------------
def run_query(query):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="******",
        database="youtube_data",
        port="5432"
    )
    df = pd.read_sql_query(query, mydb)
    return df

# ----------------------------
# STREAMLIT APP
# ----------------------------
# Page config
st.set_page_config(page_title="YouTube Data App", layout="wide")

# Custom CSS for background color
page_bg = """
<style>
[data-testid="stAppViewContainer"] {
    background-color: #ADD8E6; /* Main page background */
}

[data-testid="stSidebar"] {
    background-color: #008000; /* Sidebar background */
    color: white;
}

[data-testid="stHeader"] {
    background-color: #ff4b4b; /* Top bar */
}

h1, h2, h3, h4, h5, h6, p, span {
    color: black; /* Text color */
}
</style>
"""

st.markdown(page_bg, unsafe_allow_html=True)
st.title("📊 YouTube Data Harvesting & Warehousing")
channel_id = st.text_input("Enter YouTube Channel ID:")

if st.button("Collect & Store Data"):
    coll1 = db["channels"]
    existing_ids = [ch["Channel_Id"] for ch in coll1.find({}, {"_id":0, "Channel_Id":1})]
    if channel_id in existing_ids:
        st.warning("Channel data already exists in MongoDB")
    else:
        msg = channel_details(channel_id)
        st.success(msg)

# ----------------------------
# DISPLAY CHANNELS IN SELECTBOX
# ----------------------------
all_channels = [ch["Channel_Name"] for ch in db["channels"].find({}, {"_id":0, "Channel_Name":1})]
selected_channel = st.selectbox("Select Channel for SQL Migration:", all_channels)

# ----------------------------
# SQL MIGRATION
# ----------------------------
if st.button("Migrate to SQL"):
    def migrate_to_sql(channel_name_s):
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="******",
            database="youtube_data",
            port="5432"
        )
        cursor = mydb.cursor()

        # ----------------------------
        # CHANNELS TABLE
        # ----------------------------
        cursor.execute('''CREATE TABLE IF NOT EXISTS channels(
                            Channel_Name VARCHAR(150),
                            Channel_Id VARCHAR(80) PRIMARY KEY,
                            Subscribers BIGINT,
                            Views BIGINT,
                            Total_Videos INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(80)
                        );''')
        mydb.commit()

        ch_data = db["channels"].find_one({"Channel_Name": channel_name_s}, {"_id":0})
        cursor.execute('''INSERT INTO channels(Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
                          VALUES(%s,%s,%s,%s,%s,%s,%s)
                          ON CONFLICT (Channel_Id) DO NOTHING;''',
                       (ch_data.get("Channel_Name"), ch_data.get("Channel_Id"), ch_data.get("Subscribers"),
                        ch_data.get("Views"), ch_data.get("Total_Videos"), ch_data.get("Channel_Description"),
                        ch_data.get("Playlist_Id")))
        mydb.commit()

        # ----------------------------
        # PLAYLISTS TABLE
        # ----------------------------
        cursor.execute('''CREATE TABLE IF NOT EXISTS playlists(
                            Playlist_Id VARCHAR(100) PRIMARY KEY,
                            Title VARCHAR(150),
                            Channel_Id VARCHAR(80),
                            Channel_Name VARCHAR(150),
                            PublishedAt TIMESTAMP,
                            Video_Count INT
                        );''')
        mydb.commit()

        pl_data = db["playlists"].find({"Channel_Name": channel_name_s}, {"_id":0})
        for row in pl_data:
            cursor.execute('''INSERT INTO playlists(Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
                              VALUES(%s,%s,%s,%s,%s,%s)
                              ON CONFLICT (Playlist_Id) DO NOTHING;''',
                           (row.get("Playlist_Id"), row.get("Title"), row.get("Channel_Id"),
                            row.get("Channel_Name"), row.get("PublishedAt"), row.get("Video_Count")))
        mydb.commit()

        # ----------------------------
        # VIDEOS TABLE
        # ----------------------------
        cursor.execute('''CREATE TABLE IF NOT EXISTS videos(
                            Channel_Name VARCHAR(150),
                            Channel_Id VARCHAR(80),
                            Video_Id VARCHAR(50) PRIMARY KEY,
                            Title TEXT,
                            Tags TEXT,
                            Thumbnail TEXT,
                            Description TEXT,
                            Published_Date TIMESTAMP,
                            Duration TEXT,
                            Views BIGINT,
                            Likes BIGINT,
                            Comments BIGINT,
                            Favorite_Count BIGINT,
                            Definition VARCHAR(20),
                            Caption_Status VARCHAR(50)
                        );''')
        mydb.commit()

        vi_data = db["videos"].find({"Channel_Name": channel_name_s}, {"_id":0})
        for row in vi_data:
            cursor.execute('''INSERT INTO videos(Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date,
                              Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
                              VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                              ON CONFLICT (Video_Id) DO NOTHING;''',
                           (row.get("Channel_Name"), row.get("Channel_Id"), row.get("Video_Id"), row.get("Title"),
                            str(row.get("Tags")), row.get("Thumbnail"), row.get("Description"),
                            row.get("Published_Date"), row.get("Duration"), row.get("Views"), row.get("Likes"),
                            row.get("Comments"), row.get("Favorite_Count"), row.get("Definition"), row.get("Caption_Status")))
        mydb.commit()

        # ----------------------------
        # COMMENTS TABLE
        # ----------------------------
        cursor.execute('''CREATE TABLE IF NOT EXISTS comments(
                            Comment_Id VARCHAR(100) PRIMARY KEY,
                            Video_Id VARCHAR(50),
                            Comment_Text TEXT,
                            Comment_Author TEXT,
                            Comment_Published TIMESTAMP
                        );''')
        mydb.commit()

        com_data = db["comments"].find({}, {"_id":0})
        for row in com_data:
            cursor.execute('''INSERT INTO comments(Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                              VALUES(%s,%s,%s,%s,%s)
                              ON CONFLICT (Comment_Id) DO NOTHING;''',
                           (row.get("Comment_Id"), row.get("Video_Id"), row.get("Comment_Text"),
                            row.get("Comment_Author"), row.get("Comment_Published")))
        mydb.commit()
        st.success("Migration Completed Successfully!")

    migrate_to_sql(selected_channel)

# ----------------------------
# STREAMLIT SQL QUERY QUESTIONS
import plotly.express as px

# ----------------------------
st.subheader("All Videos and Their Channels")
df_videos = run_query('SELECT Title, Channel_Name FROM videos;')
st.dataframe(df_videos)

# ----------------------------
# 2. Channels with most number of videos
# ----------------------------
st.subheader("Channels with Most Videos")
df_channels_videos = (run_query('SELECT Channel_Name, Total_Videos FROM channels ORDER BY Total_Videos DESC;'))
fig_channels_videos = px.bar(df_channels_videos, x='channel_name', y='total_videos', color='total_videos',
                             text='total_videos', title="Channels by total_videos",
                             color_discrete_sequence=px.colors.qualitative.Pastel)  # 🎨 pastel colors)
st.plotly_chart(fig_channels_videos, use_container_width=True)

# ----------------------------
# 3. 10 Most Viewed Videos
# ----------------------------
st.subheader("Top 10 Most Viewed Videos")
df_most_viewed = run_query('SELECT Title, Channel_Name, Views FROM videos WHERE Views IS NOT NULL ORDER BY Views::bigint DESC LIMIT 10;')
fig_most_viewed = px.bar(df_most_viewed, x='title', y='views', color='views', text='views', title="Top 10 Most Viewed Videos",
                         color_discrete_sequence=px.colors.qualitative.Pastel)
st.plotly_chart(fig_most_viewed, use_container_width=True)

# ----------------------------
# 4. Comments in each video
# ----------------------------
st.subheader("Comments in Each Video")
df_comments = run_query('SELECT Video_Id, Comment_Text FROM comments;')
st.dataframe(df_comments)

# ----------------------------
# 5. Videos with highest likes
# ----------------------------
st.subheader("Top 10 Most Liked Videos")
df_most_likes = run_query('SELECT Title, Channel_Name, Likes FROM videos WHERE Likes IS NOT NULL ORDER BY Likes::bigint DESC LIMIT 10;')
fig_most_likes = px.bar(df_most_likes, x='title', y='likes', color='likes', text='likes', title="Top 10 Most Liked Videos")
st.plotly_chart(fig_most_likes, use_container_width=True)

# ----------------------------
# 6. Likes of all videos
# ----------------------------
st.subheader("Total Likes per Channel")

# Aggregate likes per channel
df_likes_channel = run_query('''
    SELECT Channel_Name, SUM(Likes::bigint) AS Total_Likes
    FROM videos
    WHERE Likes IS NOT NULL
    GROUP BY Channel_Name
    ORDER BY Total_Likes DESC;
''')

# Plot the bar chart
fig_likes_channel = px.bar(
    df_likes_channel, 
    x='channel_name', 
    y='total_likes', 
    color='total_likes', 
    text='total_likes', 
    title="Total Likes for Each Channel",
    color_discrete_sequence=px.colors.qualitative.Plotly
)

st.plotly_chart(fig_likes_channel, use_container_width=True)


# ----------------------------
# 7. Views of each channel
# ----------------------------
st.subheader("Views of Each Channel")
df_views = run_query('SELECT Channel_Name, Views FROM channels;')
fig_views = px.bar(df_views, x='channel_name', y='views', color='views', text='views', title="Views per Channel",
                   color_discrete_sequence=px.colors.qualitative.Vivid)
st.plotly_chart(fig_views, use_container_width=True)

# ----------------------------
# 8. Videos published in 2022
# ----------------------------
st.subheader("Videos Published in 2025")
df_2022 = run_query("SELECT Title, Channel_Name, Published_Date FROM videos WHERE EXTRACT(YEAR FROM Published_Date) = 2025;")
st.dataframe(df_2022)

# ----------------------------
# 9. Average duration of videos per channel (in hours)
# ----------------------------
st.subheader("Average Duration of Videos per Channel (Hours)")

df_avg_duration = run_query("""
    SELECT Channel_Name, 
           ROUND(AVG(EXTRACT(EPOCH FROM Duration::interval) / 3600), 2) AS Average_Duration_Hours
    FROM videos
    GROUP BY Channel_Name;
""")

fig_avg_duration = px.bar(
    df_avg_duration,
    x="channel_name",
    y="average_duration_hours",
    color="channel_name",
    text="average_duration_hours",
    title="Average Video Duration per Channel (Hours)",
    color_discrete_sequence=px.colors.qualitative.Vivid
)

fig_avg_duration.update_traces(textposition="outside")
fig_avg_duration.update_layout(showlegend=False)

st.plotly_chart(fig_avg_duration, use_container_width=True)

# ----------------------------
# 10. Videos with highest number of comments
# ----------------------------
st.subheader("Top 10 Videos with Highest Comments")
df_top_comments = run_query("SELECT Title, Channel_Name, Comments FROM videos WHERE Comments IS NOT NULL ORDER BY Comments::bigint DESC LIMIT 10;")
fig_top_comments = px.bar(df_top_comments, x='title', y='comments', color='comments', text='comments',
                          title="Top 10 Videos by Comments", color_discrete_sequence=px.colors.qualitative.Vivid)

st.plotly_chart(fig_top_comments, use_container_width=True)
