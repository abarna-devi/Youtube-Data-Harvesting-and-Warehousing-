import googleapiclient.discovery
import pandas as pd
import mysql.connector as mysql
from datetime import datetime
import streamlit as st
import json

# API setup
API_KEY = "YOU API KEY"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY)



# Function to get channel data
def get_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for i in response["items"]:
        channel_info = {
            'Channel_Name': i["snippet"]["title"],
            'Channel_Id': channel_id,
            'Subscription_Count': i["statistics"].get("subscriberCount", 0),
            'Channel_Views': i["statistics"].get("viewCount", 0),
            'Channel_Description': i["snippet"]["localized"]["description"],
            'Playlist_Id': i["contentDetails"]["relatedPlaylists"]["uploads"]
        }
    return channel_info

# Function to get video IDs
def get_videos_ids(channel_id):
    video_ids = []
    try:
        response = youtube.channels().list(
            id=channel_id,
            part="contentDetails"
        ).execute()
        playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        next_page_token = None
        while True:
            response = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            for i in range(len(response["items"])):
                video_ids.append(response["items"][i]["snippet"]["resourceId"]["videoId"])
            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                break

    except KeyError:
        print("Error: No 'items' key found in the response. The channel may not exist or may not be accessible.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return video_ids

# Function to get video data
def get_videos_data(video_ids):
    video_details = []
    for video_id in video_ids:
        try:
            response = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            ).execute()

            for item in response["items"]:
                if "id" in item:
                    video_info = {
                        "Video_Id": item["id"],
                        "Channel_Id": item["snippet"]["channelId"],
                        "Video_Name": item["snippet"]["title"],
                        "Video_Description": item["snippet"]["description"],
                        "Tags": item["snippet"].get("tags", []),
                        "PublishedAt": item["snippet"]["publishedAt"],
                        "View_Count": item["statistics"].get("viewCount", 0),
                        "Like_Count": item["statistics"].get("likeCount", 0),
                        "Dislike_Count": item["statistics"].get("dislikeCount", 0),
                        "Favorite_Count": item["statistics"].get("favoriteCount", 0),
                        "Comment_Count": item["statistics"].get("commentCount", 0),
                        "Duration": item["contentDetails"]["duration"],
                        "Thumbnail": item["snippet"]["thumbnails"]["default"]["url"],
                        "Caption_Status": item["contentDetails"].get("caption", False),
                    }
                    video_details.append(video_info)
                else:
                    print("Error: 'id' attribute not found in video data.")

        except KeyError as e:
            print(f"KeyError: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
    return video_details

# Function to get comments data
def get_comments_data(video_id):
    comment_details = []
    next_page_token = None

    try:
        while True:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            # Check if comments are disabled for the video
            if 'items' not in response:
                print(f"Comments are disabled for the video with ID: {video_id}")
                return []

            for com in response["items"]:
                comment_info = {
                    "Comment_Id": com["id"],
                    "Video_Id": video_id,
                    "Comment_Text": com["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "Comment_Author": com["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "Comment_PublishedAt": com["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                }
                comment_details.append(comment_info)

            # Update next_page_token for pagination
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break

    except Exception as e:
        print(f"An error occurred: {e}")

    return comment_details

# Establish database connection
conn = mysql.connect(
    host="localhost",
    user="root",
    password="YOUR PASSWORD",
    port=3306,  # Port should be an integer
    database="YOUR DATABASE"
)


# Helper functions
def parse_published_date(published_at):
    return pd.to_datetime(published_at)

def parse_duration(duration_str):
    hours, minutes, seconds = 0, 0, 0
    duration_str = duration_str.replace('PT', '')
    if 'H' in duration_str:
        hours, duration_str = duration_str.split('H')
        hours = int(hours)
    if 'M' in duration_str:
        minutes, duration_str = duration_str.split('M')
        minutes = int(minutes)
    if 'S' in duration_str:
        seconds = duration_str.split('S')[0]
        seconds = int(seconds)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def table_channel_data(channel_ids,conn):
    channel_list = []

    channel_info = get_channel_data(channel_ids)
    channel_list.append(channel_info)
        
        # Create df from channel data
    df_channel_data = pd.DataFrame(channel_list)
   
    cursor=conn.cursor()

    try:
            # Create channels table if not exists
        cursor.execute("""CREATE TABLE IF NOT EXISTS channels (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100) PRIMARY KEY,
            Subscription_Count BIGINT,
            Channel_Views BIGINT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(100))""")
        conn.commit()

        # Insert channel data into the database
        for index, row in  df_channel_data.iterrows():
            cursor.execute("""INSERT INTO channels (Channel_Name, Channel_Id, Subscription_Count, 
                           Channel_Views, Channel_Description, Playlist_Id)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                Channel_Name = VALUES(Channel_Name),
                                Subscription_Count = VALUES(Subscription_Count),
                                Channel_Views = VALUES(Channel_Views),
                                Channel_Description = VALUES(Channel_Description),
                                Playlist_Id = VALUES(Playlist_Id)""",
                                (row['Channel_Name'], row['Channel_Id'], row['Subscription_Count'], 
                                 row['Channel_Views'], row['Channel_Description'], row['Playlist_Id']))
            
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    return df_channel_data

def table_video_data(channel_ids, conn):
    video_list = []
    for channel_id in channel_ids:
        video_ids = get_videos_ids(channel_id)
        video_data = get_videos_data(video_ids)
        video_list.extend(video_data)

    # Create DataFrame with video data
    df_video_data = pd.DataFrame(video_list)
    cursor = conn.cursor()
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS videos (
            Video_Id VARCHAR(50) PRIMARY KEY,
            Channel_Id VARCHAR(100),
            Video_Name VARCHAR(255),
            Video_Description TEXT,
            Tags TEXT,
            PublishedAt DATETIME,
            View_Count BIGINT,
            Like_Count BIGINT,
            Dislike_Count BIGINT,
            Favorite_CountBIGINT,
            Comment_Count BIGINT,
            Duration TIME,
            Thumbnail VARCHAR(255),
            Caption_Status BOOLEAN,
            FOREIGN KEY (Channel_Id) REFERENCES channels(Channel_Id)
        )""")
        conn.commit()

        for index, row in df_video_data.iterrows():
                published_at = parse_published_date(row['PublishedAt'])
                duration = parse_duration(row['Duration'])
                caption_status = 1 if row['Caption_Status'] else 0

                cursor.execute("""INSERT INTO videos (Video_Id, Channel_Id, Video_Name, Video_Description, Tags, PublishedAt, 
                                                        View_Count, Like_Count, Dislike_Count, Favorite_Count, Comment_Count,
                                                        Duration, Thumbnail, Caption_Status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                Channel_Id = VALUES(Channel_Id),
                                Video_Name = VALUES(Video_Name),
                                Video_Description = VALUES(Video_Description),
                                Tags = VALUES(Tags),
                                PublishedAt = VALUES(PublishedAt),
                                View_Count = VALUES(View_Count),
                                Like_Count = VALUES(Like_Count),
                                Dislike_Count = VALUES(Dislike_Count),
                                Favorite_Count = VALUES(Favorite_Count),
                                Comment_Count = VALUES(Comment_Count),
                                Duration = VALUES(Duration),
                                Thumbnail = VALUES(Thumbnail),
                                Caption_Status = VALUES(Caption_Status)""",
                                (row['Video_Id'], row['Channel_Id'], row['Video_Name'], row['Video_Description'], ','.join(row['Tags']),
                                published_at.strftime('%Y-%m-%d %H:%M:%S'), row['View_Count'], row['Like_Count'], row['Dislike_Count'],
                                row['Favorite_Count'], row['Comment_Count'], duration, row['Thumbnail'], caption_status))

        conn.commit()
    except Exception as e:
        print(f"An error occurred while inserting video data: {e}")
        conn.rollback()

    return df_video_data

def table_comment_data(video_id, conn):
    comment_list = []
    comments_info = get_comments_data(video_id)
    comment_list.extend(comments_info)
    df_comment_data = pd.DataFrame(comment_list)
    
    cursor = conn.cursor()

    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS comments (
                            Comment_Id VARCHAR(50) PRIMARY KEY,
                            Video_Id VARCHAR(50),
                            Comment_Text TEXT,
                            Comment_Author VARCHAR(255),
                            Comment_PublishedAt DATETIME,
                            FOREIGN KEY (Video_Id) REFERENCES videos(Video_Id)
                          )""")
        conn.commit()

        for index, row in df_comment_data.iterrows():
            comment_publishedat = parse_published_date(row['Comment_PublishedAt'])
            cursor.execute("""INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_PublishedAt)
                              VALUES (%s, %s, %s, %s, %s)
                              ON DUPLICATE KEY UPDATE
                              Video_Id = VALUES(Video_Id),
                              Comment_Text = VALUES(Comment_Text),
                              Comment_Author = VALUES(Comment_Author),
                              Comment_PublishedAt = VALUES(Comment_PublishedAt)""",
                           (row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'], 
                            comment_publishedat.strftime('%Y-%m-%d %H:%M:%S')))
            conn.commit()
    except Exception as e:
        print(f"An error occurred while inserting comment data: {e}")
        conn.rollback()


    return df_comment_data


# Establish database connection
conn = mysql.connect(
    host="localhost",
    user="root",
    password="YOUR PASSWORD",
    port=3306,
    database="YOUR DATABASE"
)
st.sidebar.title(":red[YouTube Data Harvesting and Warehousing]")
st.sidebar.header("Project Overview")
st.sidebar.write("This tool allows users to collect, store, and analyze YouTube channel, video, and comment data.")

st.sidebar.header("Key Components")
st.sidebar.caption("1. Python Scripting")
st.sidebar.caption("2. Data Collection via YouTube API")
st.sidebar.caption("3. Data Management in MySQL")

st.sidebar.header("Data Sources")
st.sidebar.write("YouTube API: For fetching channel and video data.")
st.sidebar.write("MySQL Database: For storing collected data.")

st.sidebar.header("Usage Requirements")
st.sidebar.write("To use this tool, you need:")
st.sidebar.write("- A Google API Key for accessing the YouTube API.")
st.sidebar.write("- MySQL database credentials for data storage.")

st.sidebar.header("API Key")
st.sidebar.write("Make sure to replace the `API_KEY` variable with your own YouTube API key.")

st.sidebar.header("Database Connection")
st.sidebar.write("Ensure that your MySQL database is properly configured and accessible.")

st.sidebar.header("Dependencies")
st.sidebar.write("- Streamlit")
st.sidebar.write("- MySQL Connector")
st.sidebar.write("- Google API Client")



# Function to collect and store data
def collect_and_store_data(channel_ids, conn):
    for channel_id in channel_ids:
        df_channel_data = table_channel_data(channel_id, conn)
        df_video_data = table_video_data([channel_id], conn)
        try:
            print("Video Data DataFrame:")
            print(df_video_data)
            video_ids = df_video_data['Video_Id'].tolist()
            table_comment_data(video_ids, conn)
        except KeyError as e:
            print(f"KeyError: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

# Function to retrieve and display channel data
def show_channel_data(conn):
    st.subheader("Channel Data")
    df_channel_data = pd.read_sql("SELECT * FROM channels", conn)
    st.write(df_channel_data)

# Function to retrieve and display video data
def show_video_data(conn):
    st.subheader("Video Data")
    df_video_data = pd.read_sql("SELECT * FROM videos", conn)
    st.write(df_video_data)

# Function to retrieve and display comment data
def show_comment_data(conn):
    st.subheader("Comment Data")
    df_comment_data = pd.read_sql("SELECT * FROM comments", conn)
    st.write(df_comment_data)

# Main function
def main():
    channel_ids = st.text_input("Enter the channel IDs (separated by commas)")  # Streamlit text_input
    channel_ids = channel_ids.split(',') if channel_ids else []

    if st.button("Collect and Store Data"):  # Streamlit button
        if channel_ids:
            collect_and_store_data(channel_ids, conn)
            st.success("Data collection and storage completed successfully.")  # Streamlit success message
        else:
            st.warning("Please enter channel IDs.")  # Streamlit warning message

    if st.checkbox("Channel"):  # Streamlit checkbox
        show_channel_data(conn)

    if st.checkbox("Video"):  # Streamlit checkbox
        show_video_data(conn)

    if st.checkbox("Comment"):  # Streamlit checkbox
        show_comment_data(conn)

if __name__ == "__main__":
    main()

#-------------------------------------------------------------------------------------------------------------------------------------------------



# MySQL connection
conn = mysql.connect(
    host="localhost",
    user="root",
    password="YOUR PASSWORD",
    port=3306,
    database="YOUR DATABASE"
)

cursor = conn.cursor()

# Streamlit UI
st.title("SQL Query Selector")

question = st.selectbox(":green[SELECT QUESTION]", (
    "select option",
    "1. Names of all videos and their corresponding channels.",
    "2. Channels with the most videos and their video counts.",
    "3. Top 10 most viewed videos and their respective channels.",
    "4. Number of comments on each video and their corresponding video names.",
    "5. Videos with the highest number of likes and their corresponding channel names.",
    "6. Total number of likes and dislikes for each video and their corresponding video names.",
    "7. Total number of views for each channel and their corresponding channel names.",
    "8. Channels that published videos in the year 2022.",
    "9. Average duration of all videos in each channel and their corresponding channel names.",
    "10. Videos with the highest number of comments and their corresponding channel names."
))

if st.button("Run Query"):
    if question != "select option":
        query = ""

        if question == "1. Names of all videos and their corresponding channels.":
            query = """
                SELECT videos.Video_Name AS 'Video Title', channels.Channel_Name AS 'Channel Name'
                FROM videos
                INNER JOIN channels ON videos.Channel_Id = channels.Channel_Id
            """
        elif question == "2. Channels with the most videos and their video counts.":
            query = """
                SELECT c.Channel_Name AS 'Channel Name', COUNT(v.Video_Id) AS 'No. of Videos'
                FROM channels c
                LEFT JOIN videos v ON c.Channel_Id = v.Channel_Id
                GROUP BY c.Channel_Id, c.Channel_Name
                ORDER BY 'No. of Videos' DESC
            """
        elif question == "3. Top 10 most viewed videos and their respective channels.":
            query = """
                SELECT v.View_Count AS 'Views', c.Channel_Name AS 'Channel Name', v.Video_Name AS 'Video Title'
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                WHERE v.View_Count IS NOT NULL
                ORDER BY v.View_Count DESC
                LIMIT 10
            """
        elif question == "4. Number of comments on each video and their corresponding video names.":
            query = """
                SELECT v.Video_Name AS 'Video Title', COUNT(*) AS 'No. of Comments'
                FROM videos v
                INNER JOIN comments c ON v.Video_Id = c.Video_Id
                GROUP BY v.Video_Id, v.Video_Name
            """
        elif question == "5. Videos with the highest number of likes and their corresponding channel names.":
            query = """
                SELECT v.Video_Name AS 'Video Title', v.Like_Count AS 'Like Count', c.Channel_Name AS 'Channel Name'
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                ORDER BY v.Like_Count DESC
            """
        elif question == "6. Total number of likes and dislikes for each video and their corresponding video names.":
            query = """
                SELECT v.Video_Name AS 'Video Title', SUM(v.Like_Count) AS 'Like Count'
                FROM videos
                GROUP BY v.Video_Id, v.Video_Name
            """
        elif question == "7. Total number of views for each channel and their corresponding channel names.":
           query = """
                SELECT c.Channel_Name AS 'Channel Name', SUM(v.View_Count) AS 'Total Views'
                FROM channels c
                LEFT JOIN videos v ON v.Channel_Id = c.Channel_Id
                GROUP BY c.Channel_Name
            """
               
        elif question == "8. Channels that published videos in the year 2022.":
            query = """
                SELECT DISTINCT channels.Channel_Name AS 'Channel Name'
                FROM channels
                INNER JOIN videos ON channels.Channel_Id = videos.Channel_Id
                WHERE YEAR(videos.PublishedAt) = 2022
            """
        elif question == "9. Average duration of all videos in each channel and their corresponding channel names.":
            query = """
                SELECT c.Channel_Name AS 'Channel Name', SEC_TO_TIME(AVG(TIME_TO_SEC(v.Duration))) AS 'Average Duration'
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                GROUP BY c.Channel_Id, c.Channel_Name
            """
        elif question == "10. Videos with the highest number of comments and their corresponding channel names.":
            query = """
                SELECT v.Video_Name AS 'Video Title', c.Channel_Name AS 'Channel Name', COUNT(*) AS 'Comment Count'
                FROM videos v
                INNER JOIN channels c ON v.Channel_Id = c.Channel_Id
                INNER JOIN comments com ON v.Video_Id = com.Video_Id
                GROUP BY v.Video_Name, c.Channel_Name
                ORDER BY 'Comment Count' DESC
            """

        if query:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
                st.write(df)
            else:
                st.write("No results found.")

#--------------------------------------------------------------------------------------------------------------------------------------------

