import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from datetime import timedelta
import isodate  # To convert ISO 8601 duration format to seconds
from dateutil import parser  # To parse and format date and time
import mysql.connector  # MySQL Connector for Python
import json  # To handle JSON serialization

# Initialize session state variables
if 'channel_details' not in st.session_state:
    st.session_state.channel_details = None
if 'all_playlists' not in st.session_state:
    st.session_state.all_playlists = []
if 'all_video_details' not in st.session_state:
    st.session_state.all_video_details = []
if 'all_comments' not in st.session_state:
    st.session_state.all_comments = []
if 'saved_to_mysql' not in st.session_state:
    st.session_state.saved_to_mysql = False

# API setup with your API key
API_KEY = "YOUR_API_KEY"  # Replace with your actual API key
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# MySQL credentials
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "Password123" 
DATABASE_NAME = "youtube"

# Function to connect to MySQL and create the "youtube" database and tables if they don't exist
def initialize_database():
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        cursor = connection.cursor()
        # Create database "youtube"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        cursor.execute(f"USE {DATABASE_NAME}")

        # Create tables in the "youtube" database
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel (
                Channel_Name VARCHAR(255),
                Channel_Id VARCHAR(255) PRIMARY KEY,
                Subscription_Count INT,
                Channel_Views INT,
                Channel_Description TEXT,
                Total_Videos INT,
                Total_Playlists INT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist (
                Playlist_Id VARCHAR(255) PRIMARY KEY,
                Channel_Id VARCHAR(255),
                Playlist_Name VARCHAR(255),
                Total_Videos_In_Playlist INT,
                FOREIGN KEY (Channel_Id) REFERENCES channel(Channel_Id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS video (
                Video_Id VARCHAR(255) PRIMARY KEY,
                Video_Name VARCHAR(255),
                Video_Description TEXT,
                Tags JSON,
                PublishedAt DATETIME,
                View_Count INT,
                Like_Count INT,
                Duration INT,  -- Duration stored as INT (seconds)
                Thumbnail VARCHAR(255),
                Playlist_Id VARCHAR(255),
                Playlist_Name VARCHAR(255),
                FOREIGN KEY (Playlist_Id) REFERENCES playlist(Playlist_Id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment (
                Comment_Id VARCHAR(255) PRIMARY KEY,
                Comment_Text TEXT,
                Comment_Author VARCHAR(255),
                Published_Date DATETIME,
                Video_Id VARCHAR(255),
                FOREIGN KEY (Video_Id) REFERENCES video(Video_Id) ON DELETE CASCADE
            )
        """)
        connection.commit()
        cursor.close()
        connection.close()

    except mysql.connector.Error as err:
        st.error(f"Error initializing the database: {err}")

# Initialize the database and tables on execution
initialize_database()

# Set up YouTube API client
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

# Streamlit UI
st.title("Welcome to MA31 YouTube Data Harvesting Project")

# Updated list of YouTube channels with their names and categories
channels = {
    "SIKHO COMPUTER AND TECH": "UCfi4W026NtwxmDq3BVlmnFw",
    "Digital Dollars": "UCVGETS6YDIf7C0VVY1BeC3w",
    "Divine Music": "UCyMT2daOZpmczDuFG_uYFRA",
    "Aadhira": "UCEz81Y9bCUiHwFQj_z_KQog",
    "Moconomy": "UCoOHTipX1_cNiC9seIPfUXA",
    "MotoWagon": "UC9LjrPL1bLjJ2oIU3NSdcMQ",
    "Thatz It Channel": "UCW1y9QjWHGWCMn7GPoDvoHw",
    "Tamil Factory": "UC7TB1r3QXJ1Y2rU4CU96jRQ",
    "Earnings Blueprint": "UCajgrp9PZUyrAx8RMqwuYiA",
    "Emmanuel Crown": "UCZj4IZIbT3irBiS9xICCBuA"
}

# Dropdown for selecting a YouTube channel
selected_channel_name = st.selectbox("Select a YouTube Channel", options=list(channels.keys()))

# Function to handle empty or missing values
def handle_missing_values(data, default="null"):
    return data if data else default

# Function to convert ISO 8601 date format to MySQL DATETIME format
def convert_to_mysql_datetime(iso_date):
    try:
        dt = parser.isoparse(iso_date)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

# Function to get channel details
def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    channel_data = None
    for item in response['items']:
        channel_data = {
            "Channel_Name": handle_missing_values(item["snippet"].get("title")),
            "Channel_Id": handle_missing_values(channel_id),
            "Subscription_Count": int(handle_missing_values(item["statistics"].get("subscriberCount"), 0)),
            "Channel_Views": int(handle_missing_values(item["statistics"].get("viewCount"), 0)),
            "Channel_Description": handle_missing_values(item["snippet"].get("description")),
            "Total_Videos": 0,  # Placeholder to be updated later
            "Total_Playlists": 0  # Placeholder to be updated later
        }
    return channel_data

# Function to get all playlist details
def get_all_playlist_details(youtube, channel_id):
    playlists = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,  # Fetch up to 50 playlists per request
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response.get('items', []):
            playlist_data = {
                "Playlist_Id": handle_missing_values(item["id"]),
                "Channel_Id": handle_missing_values(item["snippet"].get("channelId")),
                "Playlist_Name": handle_missing_values(item["snippet"].get("title")),
                "Total_Videos_In_Playlist": int(handle_missing_values(item["contentDetails"].get("itemCount"), 0))
            }
            playlists.append(playlist_data)
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return playlists

# Function to get all video details
def get_all_video_details(youtube, playlist_id, playlist_name):
    videos = []
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        video_ids = [item['contentDetails']['videoId'] for item in response.get('items', [])]
        if not video_ids:
            break

        video_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids)
        )
        video_response = video_request.execute()

        for item in video_response.get('items', []):
            iso_duration = item["contentDetails"].get("duration", "null")
            try:
                duration_seconds = int(isodate.parse_duration(iso_duration).total_seconds())
            except:
                duration_seconds = 0

            published_at = item["snippet"].get("publishedAt", "null")
            published_date_time = convert_to_mysql_datetime(published_at)

            tags = item["snippet"].get("tags", [])
            tags_json = json.dumps(tags) if tags else json.dumps([])

            video_data = {
                "Video_Id": handle_missing_values(item["id"]),
                "Video_Name": handle_missing_values(item["snippet"].get("title")),
                "Video_Description": handle_missing_values(item["snippet"].get("description")),
                "Tags": tags_json,
                "PublishedAt": handle_missing_values(published_date_time),
                "View_Count": int(handle_missing_values(item["statistics"].get("viewCount"), 0)),
                "Like_Count": int(handle_missing_values(item["statistics"].get("likeCount"), 0)),
                "Duration": duration_seconds,
                "Thumbnail": handle_missing_values(item["snippet"]["thumbnails"]["default"].get("url")),
                "Playlist_Id": handle_missing_values(playlist_id),
                "Playlist_Name": handle_missing_values(playlist_name)
            }
            videos.append(video_data)
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return videos

# Function to get comments for each video
def get_video_comments(youtube, video_id):
    comments = []
    next_page_token = None
    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            for item in response.get('items', []):
                comment_data = item["snippet"]["topLevelComment"]["snippet"]
                comment = {
                    "Comment_Id": handle_missing_values(item["id"]),
                    "Comment_Text": handle_missing_values(comment_data.get("textDisplay")),
                    "Comment_Author": handle_missing_values(comment_data.get("authorDisplayName")),
                    "Published_Date": convert_to_mysql_datetime(comment_data.get("publishedAt")),
                    "Video_Id": handle_missing_values(video_id)
                }
                comments.append(comment)
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            # No comments available or comments disabled
            break
    return comments

# When a channel is selected
if selected_channel_name:
    channel_id = channels[selected_channel_name]
    st.write(f"Selected Channel: *{selected_channel_name}*")

    # Fetch and display channel details
    if st.button("Fetch Data"):
        # Fetch data and store in session_state
        st.session_state.channel_details = get_channel_details(youtube, channel_id)
        st.session_state.all_playlists = get_all_playlist_details(youtube, channel_id)
        # Calculate total number of videos from all playlists
        total_videos_count = sum(playlist["Total_Videos_In_Playlist"] for playlist in st.session_state.all_playlists)
        # Update the channel details with the correct total video count
        st.session_state.channel_details["Total_Videos"] = total_videos_count
        st.session_state.channel_details["Total_Playlists"] = len(st.session_state.all_playlists)

        st.session_state.all_video_details = []
        st.session_state.all_comments = []

        for playlist in st.session_state.all_playlists:
            playlist_id = playlist['Playlist_Id']
            playlist_name = playlist['Playlist_Name']
            videos = get_all_video_details(youtube, playlist_id, playlist_name)
            st.session_state.all_video_details.extend(videos)

            for video in videos:
                video_id = video["Video_Id"]
                comments = get_video_comments(youtube, video_id)
                st.session_state.all_comments.extend(comments)

        # Display dataframes
        # Convert to DataFrame for tabular display
        channel_df = pd.DataFrame([st.session_state.channel_details])
        playlist_df = pd.DataFrame(st.session_state.all_playlists)
        video_df = pd.DataFrame(st.session_state.all_video_details)
        comment_df = pd.DataFrame(st.session_state.all_comments)

        st.write("*Channel Details:*")
        st.dataframe(channel_df)
        st.write("*Playlist Details:*")
        st.dataframe(playlist_df)
        st.write("*Video Details:*")
        st.dataframe(video_df)
        st.write(f"*Total Comments Fetched: {len(st.session_state.all_comments)}*")
        st.dataframe(comment_df)

        # Reset saved_to_mysql flag
        st.session_state.saved_to_mysql = False

    # Display "Save to MySQL" button after fetching data
    if st.session_state.channel_details:
        if st.button("Save to MySQL"):
            # Insert data into MySQL
            try:
                # Insert channel data
                connection = mysql.connector.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=DATABASE_NAME
                )
                cursor = connection.cursor()
                channel_data = st.session_state.channel_details
                channel_data_tuple = (
                    channel_data["Channel_Name"], channel_data["Channel_Id"],
                    channel_data["Subscription_Count"], channel_data["Channel_Views"],
                    channel_data["Channel_Description"], channel_data["Total_Videos"],
                    channel_data["Total_Playlists"]
                )
                cursor.execute("""
                    INSERT INTO channel (Channel_Name, Channel_Id, Subscription_Count, Channel_Views, Channel_Description, Total_Videos, Total_Playlists)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    Subscription_Count = VALUES(Subscription_Count),
                    Channel_Views = VALUES(Channel_Views),
                    Channel_Description = VALUES(Channel_Description),
                    Total_Videos = VALUES(Total_Videos),
                    Total_Playlists = VALUES(Total_Playlists)
                """, channel_data_tuple)
                connection.commit()

                # Insert playlists
                for playlist_data in st.session_state.all_playlists:
                    playlist_data_tuple = (
                        playlist_data["Playlist_Id"], playlist_data["Channel_Id"],
                        playlist_data["Playlist_Name"], playlist_data["Total_Videos_In_Playlist"]
                    )
                    cursor.execute("""
                        INSERT INTO playlist (Playlist_Id, Channel_Id, Playlist_Name, Total_Videos_In_Playlist)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        Playlist_Name = VALUES(Playlist_Name),
                        Total_Videos_In_Playlist = VALUES(Total_Videos_In_Playlist)
                    """, playlist_data_tuple)
                connection.commit()

                # Insert videos
                for video_data in st.session_state.all_video_details:
                    video_data_tuple = (
                        video_data["Video_Id"], video_data["Video_Name"], video_data["Video_Description"],
                        video_data["Tags"],
                        video_data["PublishedAt"], video_data["View_Count"], video_data["Like_Count"],
                        video_data["Duration"], video_data["Thumbnail"], video_data["Playlist_Id"],
                        video_data["Playlist_Name"]
                    )
                    cursor.execute("""
                        INSERT INTO video (Video_Id, Video_Name, Video_Description, Tags, PublishedAt, View_Count, Like_Count, Duration, Thumbnail, Playlist_Id, Playlist_Name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        Video_Name = VALUES(Video_Name),
                        Video_Description = VALUES(Video_Description),
                        Tags = VALUES(Tags),
                        PublishedAt = VALUES(PublishedAt),
                        View_Count = VALUES(View_Count),
                        Like_Count = VALUES(Like_Count),
                        Duration = VALUES(Duration),
                        Thumbnail = VALUES(Thumbnail),
                        Playlist_Name = VALUES(Playlist_Name)
                    """, video_data_tuple)
                connection.commit()

                # Insert comments
                for comment in st.session_state.all_comments:
                    comment_data_tuple = (
                        comment["Comment_Id"], comment["Comment_Text"], comment["Comment_Author"],
                        comment["Published_Date"], comment["Video_Id"]
                    )
                    cursor.execute("""
                        INSERT INTO comment (Comment_Id, Comment_Text, Comment_Author, Published_Date, Video_Id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        Comment_Text = VALUES(Comment_Text),
                        Comment_Author = VALUES(Comment_Author),
                        Published_Date = VALUES(Published_Date)
                    """, comment_data_tuple)
                connection.commit()

                cursor.close()
                connection.close()

                st.success("Saved Successfully!")
                st.session_state.saved_to_mysql = True

            except mysql.connector.Error as err:
                st.error(f"Error saving data: {err}")
                st.session_state.saved_to_mysql = False

    # After saving to MySQL, display dropdown
    if st.session_state.saved_to_mysql:
        # Display dropdown with query options
        query_options = [
            "What are the names of all the videos and their corresponding channels?",
            "Which channels have the most number of videos, and how many videos do they have?",
            "What are the top 10 most viewed videos and their respective channels?",
            "How many comments were made on each video, and what are their corresponding video names?",
            "Which videos have the highest number of likes, and what are their corresponding channel names?",
            "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "What is the total number of views for each channel, and what are their corresponding channel names?",
            "What are the names of all the channels that have published videos in the year 2022?",
            "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "Which videos have the highest number of comments, and what are their corresponding channel names?"
        ]

        selected_query = st.selectbox("Select a query", options=query_options)

        if selected_query:
            try:
                connection = mysql.connector.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=DATABASE_NAME
                )
                cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to get column names

                if selected_query == "What are the names of all the videos and their corresponding channels?":
                    query = """
                    SELECT v.Video_Name, c.Channel_Name
                    FROM video v
                    JOIN playlist p ON v.Playlist_Id = p.Playlist_Id
                    JOIN channel c ON p.Channel_Id = c.Channel_Id
                    """
                elif selected_query == "Which channels have the most number of videos, and how many videos do they have?":
                    query = """
                    SELECT c.Channel_Name, COUNT(v.Video_Id) AS Video_Count
                    FROM channel c
                    JOIN playlist p ON c.Channel_Id = p.Channel_Id
                    JOIN video v ON p.Playlist_Id = v.Playlist_Id
                    GROUP BY c.Channel_Name
                    ORDER BY Video_Count DESC
                    """
                elif selected_query == "What are the top 10 most viewed videos and their respective channels?":
                    query = """
                    SELECT v.Video_Name, c.Channel_Name, v.View_Count
                    FROM video v
                    JOIN playlist p ON v.Playlist_Id = p.Playlist_Id
                    JOIN channel c ON p.Channel_Id = c.Channel_Id
                    ORDER BY v.View_Count DESC
                    LIMIT 10
                    """
                elif selected_query == "How many comments were made on each video, and what are their corresponding video names?":
                    query = """
                    SELECT v.Video_Name, COUNT(cm.Comment_Id) AS Comment_Count
                    FROM video v
                    LEFT JOIN comment cm ON v.Video_Id = cm.Video_Id
                    GROUP BY v.Video_Id, v.Video_Name
                    """
                elif selected_query == "Which videos have the highest number of likes, and what are their corresponding channel names?":
                    query = """
                    SELECT v.Video_Name, c.Channel_Name, v.Like_Count
                    FROM video v
                    JOIN playlist p ON v.Playlist_Id = p.Playlist_Id
                    JOIN channel c ON p.Channel_Id = c.Channel_Id
                    ORDER BY v.Like_Count DESC
                    """
                elif selected_query == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
                    # Dislike count is deprecated; only likes are available
                    query = """
                    SELECT v.Video_Name, v.Like_Count
                    FROM video v
                    """
                elif selected_query == "What is the total number of views for each channel, and what are their corresponding channel names?":
                    query = """
                    SELECT Channel_Name, Channel_Views
                    FROM channel
                    """
                elif selected_query == "What are the names of all the channels that have published videos in the year 2022?":
                    query = """
                    SELECT DISTINCT c.Channel_Name
                    FROM channel c
                    JOIN playlist p ON c.Channel_Id = p.Channel_Id
                    JOIN video v ON p.Playlist_Id = v.Playlist_Id
                    WHERE YEAR(v.PublishedAt) = 2022
                    """
                elif selected_query == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                    query = """
                    SELECT c.Channel_Name, 
                    SEC_TO_TIME(AVG(v.Duration)) AS Average_Duration
                    FROM video v
                    JOIN playlist p ON v.Playlist_Id = p.Playlist_Id
                    JOIN channel c ON p.Channel_Id = c.Channel_Id
                    GROUP BY c.Channel_Name
                    """
                elif selected_query == "Which videos have the highest number of comments, and what are their corresponding channel names?":
                    query = """
                    SELECT v.Video_Name, c.Channel_Name, COUNT(cm.Comment_Id) AS Comment_Count
                    FROM video v
                    JOIN playlist p ON v.Playlist_Id = p.Playlist_Id
                    JOIN channel c ON p.Channel_Id = c.Channel_Id
                    LEFT JOIN comment cm ON v.Video_Id = cm.Video_Id
                    GROUP BY v.Video_Id, v.Video_Name, c.Channel_Name
                    ORDER BY Comment_Count DESC
                    """
                else:
                    query = ""

                if query:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    df = pd.DataFrame(results)
                    st.write(f"**Result:**")
                    st.dataframe(df)
                else:
                    st.error("Invalid query selected.")

                cursor.close()
                connection.close()
            except mysql.connector.Error as err:
                st.error(f"Error executing query: {err}")
