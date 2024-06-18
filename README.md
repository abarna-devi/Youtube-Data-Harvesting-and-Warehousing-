# YouTube Data Harvesting and Warehousing


### Project Overview
This project allows users to collect, store, and analyze YouTube channel, video, and comment data. It utilizes the YouTube Data API to fetch data and stores it in a MySQL database. The collected data can be queried and displayed using Streamlit.

### Key Components
Python Scripting: Core functionality implemented using Python.
Data Collection via YouTube API: Fetches data from YouTube channels, videos, and comments.
Data Management in MySQL: Stores collected data in a MySQL database.
Data Sources
YouTube API: Used for fetching channel, video, and comment data.
MySQL Database: Used for storing the collected data.
Usage Requirements
To use this tool, you need:

A Google API Key for accessing the YouTube API.
MySQL database credentials for data storage.
Dependencies
google-api-python-client
pandas
mysql-connector-python
streamlit
Setup and Installation



Install the required Python packages:

pip install google-api-python-client pandas mysql-connector-python streamlit
Set up the MySQL database:
Ensure MySQL server is running and create a database.
Update the MySQL connection details in the script.
Get a YouTube API Key:
Follow the instructions here to get your API key. Replace the placeholder API_KEY in the script with your actual API key.

Running the Application
Run the Streamlit application:

streamlit run your_script_name.py
Interacting with the Application:
Enter the YouTube channel IDs to collect data.
Use the checkboxes to display collected data (Channels, Videos, Comments).
Run SQL queries to fetch specific insights.


### Functions
Data Collection and Storage
get_channel_data(channel_id): Fetches data for a given YouTube channel.
get_videos_ids(channel_id): Retrieves all video IDs for a given channel.
get_videos_data(video_ids): Fetches data for a list of video IDs.
get_comments_data(video_id): Retrieves comments for a given video ID.
Database Functions
table_channel_data(channel_ids, conn): Stores channel data in the database.
table_video_data(channel_ids, conn): Stores video data in the database.
table_comment_data(video_id, conn): Stores comment data in the database.
Streamlit Functions
collect_and_store_data(channel_ids, conn): Collects and stores data for given channel IDs.
show_channel_data(conn): Displays channel data from the database.
show_video_data(conn): Displays video data from the database.
show_comment_data(conn): Displays comment data from the database.


### SQL Query Selector
A Streamlit UI component to run predefined SQL queries on the stored data, such as:

Names of all videos and their corresponding channels.
Channels with the most videos and their video counts.
Top 10 most viewed videos and their respective channels.
Number of comments on each video and their corresponding video names.
Videos with the highest number of likes and their corresponding channel names.
Total number of likes and dislikes for each video and their corresponding video names.
Total number of views for each channel and their corresponding channel names.
Channels that published videos in the year 2022.
Average duration of all videos in each channel and their corresponding channel names.
Videos with the highest number of comments and their corresponding channel names.
Contributing
Contributions are welcome! Please create a pull request or open an issue to discuss any changes.

### License
This project is licensed under the MIT License.

### Contributing
Contributions are welcome! Please create a pull request or open an issue to discuss any changes.



