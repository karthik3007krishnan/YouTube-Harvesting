import googleapiclient.discovery
from googleapiclient.discovery import build
import pymongo 
import psycopg2
import pandas as pd
import json
import streamlit as st 


api_service_name = "youtube"
api_version = "v3"
client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

Api_Key = "AIzaSyA44pT-IRb6JjdazazOd6zdoZ95JrrzLlQ"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey= Api_Key)


# Get channel infos
def get_ch_info(Channel_ID):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= Channel_ID
    )
    response = request.execute()


    for results in response["items"]:
        d1 = dict(Channel_name = results['snippet']['title'],
                channel_id = results['id'],
                Subscription_Count = results['statistics']['subscriberCount'],
                Channel_Views = results['statistics']['viewCount'],
                Channel_Description = results['snippet']['description']
                )
    return d1


# Get videoID infos
def get_videoid_info(Channel_ID):
    video_IDS = []

    request = youtube.channels().list(
        part="contentDetails",
        id=Channel_ID
    )
    response = request.execute()

    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        playlist_items_request = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        playlist_items_response = playlist_items_request.execute()

        for item in playlist_items_response['items']:
            video_IDS.append(item['snippet']['resourceId']['videoId'])

        next_page_token = playlist_items_response.get('nextPageToken')

        if not next_page_token:
            break

    return video_IDS

# Usage
# channel_id = "UCniKMtxy7wpM7rO58qP177w"
# video_ids = get_video_ids(channel_id)
# print(video_ids)





#Get video info
def get_video_info(Channel_ID):
    All_video_info = []

    for video_info in Channel_ID:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_info
        )
        response = request.execute()

        for item in response["items"]:
            d2 = dict(
                Video_Id=item['id'],
                Channel_Name=item['snippet']['channelTitle'],
                Channel_ID=item['snippet']['channelId'],
                Video_Name=item['snippet']['title'],
                Video_Description=item['snippet'].get('description'),
                Tags=item['snippet'].get('tags'),
                PublishedAt=item['snippet']['publishedAt'],
                View_Count=item['statistics'].get('viewCount'),
                Like_Count=item['statistics'].get('likeCount'),
                Dislike_Count=item['statistics'].get('dislikeCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Comment_Count=item['statistics'].get('commentCount'),
                Duration=item['contentDetails']['duration'],
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                caption_status=item['contentDetails']['caption']
            )
            All_video_info.append(d2)

    return All_video_info




def get_comments_info(Channel_ID):
    All_comment_info = []
    try:
        for c in Channel_ID:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=c,
                maxResults = 50
            )
            response = request.execute()

            for results in response['items']:
                d3 = dict(Comment_Id_1 = results['snippet']['topLevelComment']['id'],
                        video_Id = results['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text = results['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author = results['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt = results['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                
                All_comment_info.append(d3)
                
    except:
        pass
    return All_comment_info




def get_playlistID_info(Channel_ID):
    Initial_p_Token = None
    playlist_ids = []

    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId= Channel_ID,
                maxResults= 50,
                pageToken = Initial_p_Token
            )
        response = request.execute()

        for p_ID in response['items']:
            d4 = dict(
            channel_Name = p_ID['snippet']['channelTitle'],
            channel_id = p_ID['snippet']['channelId'],
            playlist_name = p_ID['snippet']['title'],
            playlist_id = p_ID['id'])

            playlist_ids.append(d4)
        
        Initial_p_Token = response.get('nextPageToken')
        if Initial_p_Token is None:
            break

    return playlist_ids


#Transfering data into mongodb
client = pymongo.MongoClient("mongodb+srv://karthikkriz:kk07ch30@cluster0.setsqbw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
database = client["capproj"]
collections = database['YoutubeChannelDetails']



def Channel_Full_Details(channel_ID):
    channel_details = get_ch_info(channel_ID)
    playlist_details = get_playlistID_info(channel_ID)
    All_Video_IDs = get_videoid_info(channel_ID)
    video_details = get_video_info(channel_ID)
    comments_details = get_comments_info(channel_ID)




    collections.insert_many({
        "channel": channel_details,
        "playlist": playlist_details,
        "video": video_details,
        "comments": comments_details
    })

    return "Transfering is completed"





#channel creation in sql and mongodb
def All_ch_table():
    mydb = psycopg2.connect(
        host = "localhost",
        user = "postgres",
        password = "kk07ch30",
        database = "capproj",
        port = "5432"
    )

    mycursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_name varchar(30),
                                                            channel_id varchar(30) primary key,
                                                            Subscription_Count bigint,
                                                            Channel_Views bigint,
                                                            Channel_Description text
                                                            )'''
        
        mycursor.execute(create_query)
        mydb.commit()
    except:
        print("Table is already created")


    c_list = []
    database = client["capproj"]
    collections = database["YoutubeChannelDetails"]

    for c in collections.find({},{"_id": 0, "channel": 1}):
        c_list.append(c['channel'])

    df = pd.DataFrame(c_list)    


    for index,row in df.iterrows():
            insert_query = '''insert into channels(Channel_name,
                                                channel_id,
                                                Subscription_Count,
                                                Channel_Views,
                                                Channel_Description)
                                                
                                                values(%s,%s,%s,%s,%s)'''
            
            val= (row['Channel_name'],
                row['channel_id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'])
            
            try:
                mycursor.executemany(insert_query,val)
                mydb.commit()

            except:
                print("channel details are already inserted")


#playlist creation in sql and mongodb
def All_Playist_table():
    mydb = psycopg2.connect(
        host = "localhost",
        user = "postgres",
        password = "kk07ch30",
        database = "capproj",
        port = "5432"
    )

    mycursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists playlists(
    channel_Name varchar(150),
    channel_id varchar(150),
    playlist_name varchar(200),
    playlist_id varchar(150) primary key
    )'''


    mycursor.execute(create_query)
    mydb.commit()



    play_list = []
    database = client["capproj"]
    collections = database["YoutubeChannelDetails"]

    for p in collections.find({},{"_id": 0, "playlist": 1}):
        for i in range(len(p['playlist'])):
            play_list.append(p['playlist'][i])
    df1 = pd.DataFrame(play_list)



    for index,row in df1.iterrows():
            insert_query = '''insert into playlists(
            channel_Name,
            channel_id,
            playlist_name,
            playlist_id 
            )
            
            values(%s,%s,%s,%s) '''

            
            val = (row['channel_Name'],
                row['channel_id'],
                row['playlist_name'],
                row['playlist_id'])
            
            mycursor.execute(insert_query,val)
            mydb.commit()


#video creation in sql and mongodb
def All_video_table():
    mydb = psycopg2.connect(
            host = "localhost",
            user = "postgres",
            password = "kk07ch30",
            database = "capproj",
            port = "5432"
        )

    mycursor = mydb.cursor()

    drop_query = '''drop table if exists video'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists video(Video_Id varchar(40) primary key,
                                                        Channel_Name varchar(100),
                                                        Channel_ID varchar(100),
                                                        Video_Name varchar(700),
                                                        Video_Description text,
                                                        Tags text,
                                                        PublishedAt timestamp,
                                                        View_Count bigint,
                                                        Like_Count bigint,
                                                        Dislike_Count bigint,
                                                        Favorite_Count int,
                                                        Comment_Count int,
                                                        Duration interval,
                                                        Thumbnail varchar(200),
                                                        caption_status varchar(20)
                                                        )'''
                            

    mycursor.execute(create_query)
    mydb.commit()


    vi_list = []
    database = client["capproj"]
    collections = database["YoutubeChannelDetails"]

    for v_data in collections.find({},{"_id": 0, "video": 1}):
        for i in range(len(v_data['video'])):
            vi_list.append(v_data['video'][i])
    df2 = pd.DataFrame(vi_list)



    for index, row in df2.iterrows():
            insert_query = '''insert into video(
                Video_Id,
                Channel_Name,
                Channel_ID,
                Video_Name,
                Video_Description,
                Tags,
                PublishedAt,
                View_Count,
                Like_Count,
                Dislike_Count,
                Favorite_Count,
                Comment_Count,
                Duration,
                Thumbnail,
                caption_status
            )

            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '''

            val = (
                row['Video_Id'],
                row['Channel_Name'],
                row['Channel_ID'],
                row['Video_Name'],
                row['Video_Description'],
                row['Tags'],
                row['PublishedAt'],
                row['View_Count'],
                row['Like_Count'],
                row['Dislike_Count'],
                row['Favorite_Count'],
                row['Comment_Count'],
                row['Duration'],
                row['Thumbnail'],
                row['caption_status']
            )

            mycursor.execute(insert_query, val)
            mydb.commit()


#comments creation in sql and mongodb
def All_com_table():
    mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="kk07ch30",
            database="capproj",
            port="5432"
        )

    mycursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS comments'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS comments(
            Comment_Id_1 VARCHAR(200),
            video_Id varchar(200),
            Comment_Text text,
            Comment_Author VARCHAR(100),
            Comment_PublishedAt TIMESTAMP
        )'''
    mycursor.execute(create_query)
    mydb.commit()



    com_list = []
    database = client["capproj"]
    collections = database["YoutubeChannelDetails"]

    for com_data in collections.find({}, {"_id": 0, "comments": 1}):
            for i in range(len(com_data['comments'])):
                com_list.append(com_data['comments'][i])
    df3 = pd.DataFrame(com_list)



    for index, row in df3.iterrows():
            insert_query = '''insert into comments(
                Comment_Id_1,
                video_Id,
                Comment_Text,
                Comment_Author,
                Comment_PublishedAt
            )

            values(%s,%s,%s,%s,%s) '''

            val = (
                row['Comment_Id_1'],
                row['video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_PublishedAt']
            )

            mycursor.execute(insert_query, val)
            mydb.commit()


def All_tables():
    All_ch_table()
    All_Playist_table()
    All_video_table()
    All_com_table()

    return "Tables created successfully"


def view_ch_table():
    c_list = []
    db = client["capproj"]
    collections = db["YoutubeChannelDetails"]

    for c in collections.find({}, {"_id": 0, "channel": 1}):
        c_list.append(c['channel'])

    df1 = st.dataframe(c_list)

    return df1


def view_ph_table():
    play_list = []
    db = client["capproj"]
    collections = db["YoutubeChannelDetails"]

    for p in collections.find({}, {"_id": 0, "playlist": 1}):
        if 'playlist' in p:  
            for i in range(len(p['playlist'])):
                play_list.append(p['playlist'][i])
    df2 = st.dataframe(play_list)

    return df2



def view_vi_table():
    vi_list = []
    db = client["capproj"]
    collections = db["YoutubeChannelDetails"]

    for v_data in collections.find({}, {"_id": 0, "video": 1}):
        for i in range(len(v_data['video'])):
            vi_list.append(v_data['video'][i])
    df3 = st.dataframe(vi_list)

    return df3


def view_com_table():
    com_list = []
    db = client["capproj"]
    collections = db["YoutubeChannelDetails"]

    for com_data in collections.find({}, {"_id": 0, "comments": 1}):

        for i in range(len(com_data['comments'])):
            com_list.append(com_data['comments'][i])
    df4 = st.dataframe(com_list)

    return df4




#streamlit

with st.sidebar:
    st.title(":orange[YOUTUBE SCRAPPING]")
    st.header("Inserting Channel Data")
    


channel_number_input = st.text_input("Enter the Channel ID")

if st.button("Collect and store data"):
    channel_numbers=[]
    database = client["capproj"]
    collections = database["YoutubeChannelDetails"]

    for ci in collections.find({},{"_id": 0, "channel": 1}):
        channel_numbers.append(ci['channel'])

    if channel_number_input in channel_numbers:
        st.success("Given Channel Details are Already Exists")

    else:
        insert= collections.insert_many([{"channel": {"channel_id": channel_number_input}}])

        st.success("Data stored successfully")
            

if st.button("Migrate to SQL"):
    T = All_tables()
    st.success(T)

View_table = st.radio("SELECT ANY OF THE FOLLOWING TO VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if View_table=="CHANNELS":
    view_ch_table()

elif View_table=="PLAYLISTS":
    view_ph_table()   

elif View_table=="VIDEOS":
    view_vi_table()

elif View_table=="COMMENTS":
    df_comments = view_com_table()
    st.write(df_comments)    


#sql questions

mydb = psycopg2.connect(
    host = "localhost",
    user = "postgres",
    password = "kk07ch30",
    database = "capproj",
    port = "5432"
)

mycursor = mydb.cursor()


questions = [
    "What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of channel views?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video ID's?",
    "Which video have the lowest number of likes, and what is the corresponding channel name?"]


queries = [
    '''select video_name, Channel_name from video''',
    '''select channel_name, Channel_views from channels order by channel_views desc limit 1''',
    '''select channel_name, view_count from video order by view_count desc limit 10''',
    '''select video_id, COUNT(comment_id_1) from comments group by video_id''',
    '''select channel_name, video_name, like_count from video order by like_count asc limit 1''']


st.title("SQL QUESTIONS")
selected_question = st.selectbox("Select Your Question", questions)

if selected_question:
    # Get the index of the selected question
    index = questions.index(selected_question)
    
    # Get the corresponding query
    query = queries[index]


    mycursor.execute(query)
    mydb.commit()


    results = mycursor.fetchall()


    if results:
        df = pd.DataFrame(results, columns=[col.name for col in mycursor.description])
        
        # df = pd.concat([pd.DataFrame({"Index": [""]}), df], axis=1)
        # df = pd.concat([pd.DataFrame({"Index": [""]}), df], axis=0)

        # # Set the index starting from 1
        # df.index = range(1, len(df) + 1)
        
        st.write(df)

    else:
         st.write("No results found.")


mycursor.close()
mydb.close()


   
