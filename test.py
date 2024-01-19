from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu


api_Id="AIzaSyBWhnF13pXQcjyA14GXJ_z_aExT1xB14-k"
youtube = build("youtube","v3",developerKey=api_Id)

def get_channel_id(channel_id):

    request = youtube.channels().list(part = "snippet,contentDetails,Statistics",
                                      id = channel_id).execute()

    for x in request["items"]:
        data_channel_id = dict(
                    Channel_Name = x["snippet"]["title"],
                    Channel_Id = x["id"],
                    Subscribers=x["statistics"]["subscriberCount"],
                    Views = x["statistics"]["viewCount"],
                    Total_Videos = x["statistics"]["videoCount"],
                    Channel_Description = x["snippet"]["description"],
                    Playlist_Id = x["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
    return data_channel_id
    
def playlist_info(channel_id):
    video_data = []
    next_page_token = None
    next_page = True
    while next_page:
        request = youtube.playlists().list(part="snippet,contentDetails",
                                           channelId=channel_id,maxResults=50,
                                           pageToken=next_page_token).execute()

        for x in request['items']:
            data={'PlaylistId':x['id'],
                    'Title':x['snippet']['title'],
                    'ChannelId':x['snippet']['channelId'],
                    'ChannelName':x['snippet']['channelTitle'],
                    'PublishedAt':x['snippet']['publishedAt'],
                    'VideoCount':x['contentDetails']['itemCount']}
            video_data.append(data)
        next_page_token = request.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return video_data

def get_video_ids(channel_id):
    video_ids = []
    request = youtube.channels().list(id=channel_id,part='contentDetails').execute()
    playlist_id = request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        request1 = youtube.playlistItems().list( part = 'snippet',
                                               playlistId = playlist_id,
                                               maxResults = 50,pageToken = next_page_token).execute()

        for x in request1['items']:
            video_ids.append(x['snippet']['resourceId']['videoId'])
        next_page_token = request1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

def get_video_details(video_ids):
    alldata_videoids=[]
    for video_id in video_ids:
            request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id= video_id).execute()

            for x in request["items"]:
                data = dict(Channel_Name = x['snippet']['channelTitle'],
                            Channel_Id = x['snippet']['channelId'],
                            Video_Id = x['id'],
                            Video_Name= x['snippet']['title'],
                            Description = x['snippet']['description'],
                            Tags = x['snippet'].get('tags'),
                            Published_Date = x['snippet']['publishedAt'],
                            Views = x['statistics']['viewCount'],
                            Likes = x['statistics'].get('likeCount'),
                            Comments = x['statistics'].get('commentCount'),
                            Favorite_Count = x['statistics']['favoriteCount'],
                            Thumbnail = x['snippet']['thumbnails']['default']['url'],
                            Caption_Status = x['contentDetails']['caption'],
                            Duration = x['contentDetails']['duration']
                            )
                alldata_videoids.append(data)
    return alldata_videoids

def get_comment_details(video_ids):
    comment_details=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part = "snippet",videoId =video_id,
                                                    maxResults = 50).execute()
            for x in request["items"]:
                data = dict(
                    Comment_Id = x["snippet"]["topLevelComment"]["id"],
                    Video_Id = x["snippet"]["videoId"],
                    Comment_Text = x["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    Comment_Author = x["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    Comment_Published = x["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_details.append(data)
    except:
        pass
    return comment_details

client=pymongo.MongoClient("mongodb+srv://Malavika:malu123@cluster0.camgz2z.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]

def youtube_channels_data(channel_id):
    ch_ids=get_channel_id(channel_id)
    pl_details=playlist_info(channel_id)
    vid_ids=get_video_ids(channel_id)
    vid_details=get_video_details(vid_ids)
    comment_details=get_comment_details(vid_ids)

    collect=db["youtube_channels_data"]

    collect.insert_one({"channel":ch_ids,"playlist":pl_details,"video":vid_details,"comment":comment_details})

    return "completed successfully"

def cha_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        port="5432",
                        database="Youtube")
    mycursor=mydb.cursor()

    mycursor.execute("""drop table if exists channels""")
    mydb.commit()

    try:
        mycursor.execute("""create table if not exists channels
                           (Channel_Id varchar(225) primary key,
                            Channel_Name varchar(225), 
                            Subscribers bigint, 
                            Views bigint,
                            Total_Videos int,
                            Channel_Description text,
                            Playlist_Id varchar(150))
                        """)
        mydb.commit()
    except:
        print("Channels Table already created")


    cha_data=[]
    db=client["youtube_data"]
    collect=db["youtube_channels_data"]
    for i in collect.find({},{"_id":0,"channel":1}):
        cha_data.append(i["channel"])
    df=pd.DataFrame(cha_data)

    for index,row in df.iterrows():
        insert_ch="""Insert into channels(Channel_Id,
                                        Channel_Name,
                                        Subscribers,
                                        Views,
                                        Total_Videos,
                                        Channel_Description,
                                        Playlist_Id)
                                        values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row["Channel_Id"],
                row["Channel_Name"],
                row["Subscribers"],
                row["Views"],
                row["Total_Videos"],
                row["Channel_Description"],
                row["Playlist_Id"])
        
        try:
            mycursor.execute(insert_ch,values)
            mydb.commit()
        except:
            print("Channels Table already created")

def playlis_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        port="5432",
                        database="Youtube")
    mycursor=mydb.cursor()

    mycursor.execute("""drop table if exists playlists""")
    mydb.commit()

    mycursor.execute("""create table if not exists playlists
                        (  PlaylistId varchar(225) primary key,
                            Title varchar(200),
                            ChannelId varchar(225), 
                            ChannelName varchar(200), 
                            PublishedAt timestamp,
                            VideoCount int
                        )""")
    try:
        mydb.commit()
    except:
        print("playlists table completed successfully")

    play_data=[]
    db=client["youtube_data"]
    collect=db["youtube_channels_data"]
    for i in collect.find({},{"_id":0,"playlist":1}):
        for j in range(len(i["playlist"])):
            play_data.append(i["playlist"][j])
    df=pd.DataFrame(play_data)

    for index,row in df.iterrows():
        insert_play="""Insert into playlists
                            (PlaylistId,
                            Title,
                            ChannelId,
                            ChannelName,
                            PublishedAt,
                            VideoCount)
                            values(%s,%s,%s,%s,%s,%s)"""
        values=(row["PlaylistId"],
                row["Title"],
                row["ChannelId"],
                row["ChannelName"],
                row["PublishedAt"],
                row["VideoCount"])
        
        try:
            mycursor.execute(insert_play,values)
            mydb.commit()
        except:
            print("playlists table completed sucessfully")

def vid_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        port="5432",
                        database="Youtube")
    mycursor=mydb.cursor()

    mycursor.execute("""drop table if exists videos""")
    mydb.commit()

    mycursor.execute("""create table if not exists videos
                    (   Channel_Name varchar(150),
                        Channel_Id varchar(150),
                        Video_Id varchar(160) primary key,
                        Video_Name varchar(150),
                        Description text, 
                        Tags text, 
                        Published_Date timestamp,
                        Views bigint,
                        Likes bigint,
                        Comments int,
                        Favorite_Count int,
                        Thumbnail varchar(200),
                        Caption_Status varchar(150),
                        Duration interval
                    )""")
    mydb.commit()

    video_data=[]
    db=client["youtube_data"]
    collect=db["youtube_channels_data"]
    for i in collect.find({},{"_id":0,"video":1}):
        for j in range(len(i["video"])):
            video_data.append(i["video"][j])
    df=pd.DataFrame(video_data)

    for index,row in df.iterrows():
        insert_vid="""Insert into videos
                            (Channel_Name,
                            Channel_Id,
                            Video_Id,
                            Video_Name,
                            Description,
                            Tags,
                            Published_Date,
                            Views,
                            Likes,
                            Comments,
                            Favorite_Count,
                            Thumbnail,
                            Caption_Status,
                            Duration)
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        values=(row["Channel_Name"],
                row["Channel_Id"],
                row["Video_Id"],
                row["Video_Name"],
                row["Description"],
                row["Tags"],
                row["Published_Date"],
                row["Views"],
                row["Likes"],
                row["Comments"],
                row["Favorite_Count"],
                row["Thumbnail"],
                row["Caption_Status"],
                row["Duration"])
        try:
            mycursor.execute(insert_vid,values)
            mydb.commit()
        except:
            print("videos table completed sucessfully")

def comment_table():
    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="12345",
                    port="5432",
                    database="Youtube")
    mycursor=mydb.cursor()

    mycursor.execute("""drop table if exists comments""")
    mydb.commit()

    mycursor.execute("""create table if not exists comments
                            (Comment_Id varchar(225) primary key,
                            Video_Id varchar(150),
                            Comment_Text text, 
                            Comment_Author varchar(150), 
                            Comment_Published timestamp)
                    """)
    mydb.commit()

    comt_data=[]
    db=client["youtube_data"]
    collect=db["youtube_channels_data"]
    for i in collect.find({},{"_id":0,"comment":1}):
            for j in range(len(i["comment"])):
                    comt_data.append(i["comment"][j])
    df=pd.DataFrame(comt_data)

    for index,row in df.iterrows():
        insert_comt="""Insert into comments
                                (Comment_Id,
                                Video_Id,
                                Comment_Text,
                                Comment_Author,
                                Comment_Published)
                                values(%s,%s,%s,%s,%s)"""
        values=(row["Comment_Id"],
                row["Video_Id"],
                row["Comment_Text"],
                row["Comment_Author"],
                row["Comment_Published"])
        try:
            mycursor.execute(insert_comt,values)
            mydb.commit()
        except:
            print("comments table completed sucessfully")

def all_tables():
    cha_table()
    playlis_table()
    vid_table()
    comment_table()
    
    return "tables created successfully"

def cha_table1():
    cha_data=[]
    db=client["youtube_data"]
    collect=db["youtube_channels_data"]
    for i in collect.find({},{"_id":0,"channel":1}):
        cha_data.append(i["channel"])
    ct=st.dataframe(cha_data)
    return ct

def playlis_table1():
    play_data=[]
    db=client["youtube_data"]
    collect=db["youtube_channels_data"]
    for i in collect.find({},{"_id":0,"playlist":1}):
        for j in range(len(i["playlist"])):
            play_data.append(i["playlist"][j])
    pt=st.dataframe(play_data)
    return pt

def vid_table1():
    video_data=[]
    db=client["youtube_data"]
    collect=db["youtube_channels_data"]
    for i in collect.find({},{"_id":0,"video":1}):
        for j in range(len(i["video"])):
            video_data.append(i["video"][j])
    vt=st.dataframe(video_data)
    return vt

def comment_table1():
        comt_data=[]
        db=client["youtube_data"]
        collect=db["youtube_channels_data"]
        for i in collect.find({},{"_id":0,"comment":1}):
                for j in range(len(i["comment"])):
                        comt_data.append(i["comment"][j])
        cmt=st.dataframe(comt_data)
        return cmt

#streamlit

st.set_page_config(page_title="YouTube Hub")

page_img="""<style>
[data-testid="stApp"]{
  width: 100%;
  height: 100%;
  background-size: cover;
  background-position: center center;
  background-repeat: repeat;
  background-image:url("data:image/svg+xml;utf8,%3Csvg viewBox=%220 0 2000 1000%22 xmlns=%22http:%2F%2Fwww.w3.org%2F2000%2Fsvg%22%3E%3Cmask id=%22b%22 x=%220%22 y=%220%22 width=%222000%22 height=%221000%22%3E%3Cpath fill=%22url(%23a)%22 d=%22M0 0h2000v1000H0z%22%2F%3E%3C%2Fmask%3E%3Cpath d=%22M0 0h2000v1000H0z%22%2F%3E%3Cg style=%22transform-origin:center center%22 stroke=%22%23f6000061%22 stroke-width=%222.2%22 mask=%22url(%23b)%22%3E%3Cpath fill=%22none%22 d=%22M50 0h50v50H50z%22%2F%3E%3Cpath fill=%22%23f600001f%22 d=%22M100 0h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M550 0h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001b%22 d=%22M650 0h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M700 0h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600002f%22 d=%22M750 0h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000001%22 d=%22M800 0h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600004f%22 d=%22M1100 0h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1250 0h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600000c%22 d=%22M1350 0h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1450 0h50v50h-50zM1550 0h50v50h-50zM1750 0h50v50h-50zM1850 0h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001c%22 d=%22M100 50h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600004f%22 d=%22M150 50h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M350 50h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600002f%22 d=%22M700 50h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000060%22 d=%22M950 50h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1000 50h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000042%22 d=%22M1050 50h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1100 50h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600000e%22 d=%22M1200 50h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1300 50h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000051%22 d=%22M1350 50h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1450 50h50v50h-50zM1550 50h50v50h-50zM1900 50h50v50h-50zM100 100h50v50h-50zM500 100h50v50h-50zM600 100h50v50h-50zM850 100h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000059%22 d=%22M900 100h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001c%22 d=%22M950 100h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1100 100h50v50h-50zM1250 100h50v50h-50zM1350 100h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001a%22 d=%22M1450 100h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000029%22 d=%22M1700 100h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1800 100h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600000f%22 d=%22M400 150h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M600 150h50v50h-50zM750 150h50v50h-50zM850 150h50v50h-50zM1100 150h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000039%22 d=%22M1150 150h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1200 150h50v50h-50zM1250 150h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600000a%22 d=%22M1350 150h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000039%22 d=%22M1400 150h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1800 150h50v50h-50zM1950 150h50v50h-50zM0 200h50v50H0zM150 200h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000009%22 d=%22M200 200h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M300 200h50v50h-50zM550 200h50v50h-50zM650 200h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600002d%22 d=%22M850 200h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1100 200h50v50h-50zM1400 200h50v50h-50zM1750 200h50v50h-50zM150 250h50v50h-50zM250 250h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000019%22 d=%22M400 250h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M450 250h50v50h-50zM600 250h50v50h-50zM850 250h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000009%22 d=%22M1050 250h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600000e%22 d=%22M1150 250h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600005f%22 d=%22M1250 250h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1500 250h50v50h-50zM1650 250h50v50h-50zM1750 250h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000027%22 d=%22M1900 250h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M300 300h50v50h-50zM350 300h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000028%22 d=%22M400 300h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M500 300h50v50h-50zM550 300h50v50h-50zM600 300h50v50h-50zM700 300h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000015%22 d=%22M1150 300h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1500 300h50v50h-50zM1550 300h50v50h-50zM1600 300h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600000f%22 d=%22M1650 300h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1800 300h50v50h-50zM1900 300h50v50h-50zM1950 300h50v50h-50zM150 350h50v50h-50zM200 350h50v50h-50zM300 350h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000023%22 d=%22M400 350h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001c%22 d=%22M500 350h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000036%22 d=%22M650 350h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M700 350h50v50h-50zM850 350h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600004d%22 d=%22M900 350h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1150 350h50v50h-50zM1200 350h50v50h-50zM1250 350h50v50h-50zM1300 350h50v50h-50zM1400 350h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001f%22 d=%22M1750 350h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1800 350h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600004e%22 d=%22M50 400h50v50H50z%22%2F%3E%3Cpath fill=%22%23f600005d%22 d=%22M200 400h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M350 400h50v50h-50zM450 400h50v50h-50zM500 400h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000060%22 d=%22M650 400h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M800 400h50v50h-50zM850 400h50v50h-50zM900 400h50v50h-50zM950 400h50v50h-50zM1150 400h50v50h-50zM1250 400h50v50h-50zM1300 400h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600000a%22 d=%22M1450 400h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000056%22 d=%22M1850 400h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1900 400h50v50h-50zM300 450h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000049%22 d=%22M400 450h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000011%22 d=%22M950 450h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1000 450h50v50h-50zM1100 450h50v50h-50zM1150 450h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000008%22 d=%22M1250 450h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001d%22 d=%22M1350 450h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000024%22 d=%22M1550 450h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1750 450h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000024%22 d=%22M1800 450h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M250 500h50v50h-50zM350 500h50v50h-50zM800 500h50v50h-50zM900 500h50v50h-50zM950 500h50v50h-50zM1350 500h50v50h-50zM1400 500h50v50h-50zM1450 500h50v50h-50zM1750 500h50v50h-50zM1800 500h50v50h-50zM1900 500h50v50h-50zM1950 500h50v50h-50zM50 550h50v50H50zM200 550h50v50h-50zM300 550h50v50h-50zM350 550h50v50h-50zM650 550h50v50h-50zM750 550h50v50h-50zM900 550h50v50h-50zM1050 550h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000002%22 d=%22M1200 550h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1300 550h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600005c%22 d=%22M1350 550h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1550 550h50v50h-50zM1650 550h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000041%22 d=%22M1800 550h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1900 550h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000019%22 d=%22M1950 550h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M0 600h50v50H0zM250 600h50v50h-50zM350 600h50v50h-50zM400 600h50v50h-50zM650 600h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001d%22 d=%22M1000 600h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1050 600h50v50h-50zM1100 600h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000056%22 d=%22M1150 600h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1200 600h50v50h-50zM1250 600h50v50h-50zM1300 600h50v50h-50zM1450 600h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000040%22 d=%22M1500 600h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1800 600h50v50h-50zM1850 600h50v50h-50zM1900 600h50v50h-50zM1950 600h50v50h-50zM0 650h50v50H0zM150 650h50v50h-50zM200 650h50v50h-50zM300 650h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000009%22 d=%22M350 650h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000030%22 d=%22M500 650h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M800 650h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000058%22 d=%22M850 650h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1100 650h50v50h-50zM1200 650h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600005c%22 d=%22M1250 650h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1400 650h50v50h-50zM1550 650h50v50h-50zM1700 650h50v50h-50zM1850 650h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000049%22 d=%22M100 700h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000055%22 d=%22M150 700h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M500 700h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000019%22 d=%22M550 700h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M600 700h50v50h-50zM950 700h50v50h-50zM1050 700h50v50h-50zM1200 700h50v50h-50zM1600 700h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000051%22 d=%22M1750 700h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000058%22 d=%22M1800 700h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M50 750h50v50H50zM200 750h50v50h-50zM650 750h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600002f%22 d=%22M750 750h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000046%22 d=%22M800 750h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M900 750h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000005%22 d=%22M1250 750h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000032%22 d=%22M1300 750h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1400 750h50v50h-50zM1450 750h50v50h-50zM1600 750h50v50h-50zM1900 750h50v50h-50zM1950 750h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600004c%22 d=%22M50 800h50v50H50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M300 800h50v50h-50zM600 800h50v50h-50zM650 800h50v50h-50zM850 800h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600002b%22 d=%22M1000 800h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1150 800h50v50h-50zM1250 800h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000056%22 d=%22M1400 800h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1600 800h50v50h-50zM1700 800h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000008%22 d=%22M200 850h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600002a%22 d=%22M300 850h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M400 850h50v50h-50zM650 850h50v50h-50zM1250 850h50v50h-50zM1950 850h50v50h-50zM50 900h50v50H50zM150 900h50v50h-50zM250 900h50v50h-50zM300 900h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000022%22 d=%22M450 900h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M500 900h50v50h-50zM550 900h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600003d%22 d=%22M600 900h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M700 900h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000002%22 d=%22M750 900h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M900 900h50v50h-50zM950 900h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000031%22 d=%22M1100 900h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1150 900h50v50h-50zM1200 900h50v50h-50zM1450 900h50v50h-50zM1500 900h50v50h-50zM1550 900h50v50h-50zM1600 900h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600005f%22 d=%22M1950 900h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f6000001%22 d=%22M0 950h50v50H0z%22%2F%3E%3Cpath fill=%22none%22 d=%22M100 950h50v50h-50zM450 950h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600004f%22 d=%22M500 950h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600003d%22 d=%22M650 950h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M800 950h50v50h-50zM900 950h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600001d%22 d=%22M1000 950h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1050 950h50v50h-50z%22%2F%3E%3Cpath fill=%22%23f600002f%22 d=%22M1150 950h50v50h-50z%22%2F%3E%3Cpath fill=%22none%22 d=%22M1200 950h50v50h-50zM1400 950h50v50h-50zM1450 950h50v50h-50zM1600 950h50v50h-50zM1650 950h50v50h-50zM1750 950h50v50h-50zM1950 950h50v50h-50z%22%2F%3E%3C%2Fg%3E%3Cpath fill=%22%23f3f3f3%22 filter=%22url(%23c)%22 opacity=%22.8%22 d=%22M0 0h2000v1000H0z%22%2F%3E%3Cdefs%3E%3CradialGradient id=%22a%22%3E%3Cstop offset=%220%22 stop-color=%22%23fff%22%2F%3E%3Cstop offset=%221%22 stop-color=%22%23fff%22 stop-opacity=%220%22%2F%3E%3C%2FradialGradient%3E%3Cfilter id=%22c%22 x=%22-800%22 y=%22-400%22 width=%222800%22 height=%221400%22 filterUnits=%22userSpaceOnUse%22 primitiveUnits=%22userSpaceOnUse%22 color-interpolation-filters=%22linearRGB%22%3E%3CfeTurbulence type=%22fractalNoise%22 baseFrequency=%22.105%22 numOctaves=%224%22 seed=%2215%22 stitchTiles=%22no-stitch%22 x=%220%22 y=%220%22 width=%222000%22 height=%221000%22 result=%22turbulence%22%2F%3E%3CfeSpecularLighting surfaceScale=%2210%22 specularConstant=%22.07%22 specularExponent=%2220%22 lighting-color=%22%23fff%22 x=%220%22 y=%220%22 width=%222000%22 height=%221000%22 in=%22turbulence%22 result=%22specularLighting%22%3E%3CfeDistantLight azimuth=%223%22 elevation=%22100%22%2F%3E%3C%2FfeSpecularLighting%3E%3C%2Ffilter%3E%3C%2Fdefs%3E%3C%2Fsvg%3E");
  opacity: 0.8;
}
</style>"""
st.markdown(page_img,unsafe_allow_html=True)
st.markdown("<h2 style='text-align: center; color: red;webkit-text-fill-color: black;webkit-text-stroke: 1px red;opacity:2;'>YOUTUBE DATA HARVESTING AND WAREHOUSING</h2>", unsafe_allow_html=True)

page_img1="""<style>
[data-testid="stSidebar"]{
background-image: linear-gradient( 179deg,  rgba(0,0,0,1) 9.2%, rgba(127,16,16,1) 103.9% );
background-size: cover;
opacity: 0.5;
}
</style>"""

with st.sidebar:
    st.markdown(page_img1,unsafe_allow_html=True)
    st.title(":red[YOUTUBE DATA ANALYSIS]")
    a=option_menu(None,
                  options=["HOME","EXTRACT & TRANSFORM","VIEW"],
                )
if a == "HOME":
     st.markdown("<h3 style='text-align:center; color:#F5CCA0;webkit-text-fill-color: #F5CCA0;webkit-text-stroke: 1px #6B240C;'>Welcome to the YouTube Hub </h3>",unsafe_allow_html=True)
     st.tabs(["overview"])
     st.markdown("<h5 style ='color:#FAEED1;'>Streamlit app fetches YouTube data via API, stores it in MongoDB, migrates to a SQL data warehouse. Users query the warehouse using SQL, and the results are displayed in the Streamlit app, creating a seamless experience for exploring and analyzing YouTube data.</h5>",unsafe_allow_html=True)
     st.tabs(["Technologies"])
     st.markdown("<h6 style ='color:#FAEED1;'>1.Python</h6>",unsafe_allow_html=True)
     st.markdown("<h6 style ='color:#FAEED1;'>2.Streamlit</h6>",unsafe_allow_html=True)
     st.markdown("<h6 style ='color:#FAEED1;'>3.YouTube API</h6>",unsafe_allow_html=True)
     st.markdown("<h6 style ='color:#FAEED1;'>4.MongoDB</h6>",unsafe_allow_html=True)
     st.markdown("<h6 style ='color:#FAEED1;'>5.SQL Database PostgreSQL</h6>",unsafe_allow_html=True)


elif a == "EXTRACT & TRANSFORM":
    channel_id = st.text_input("Enter the YouTube Channel ID")

    if st.button("Collect_StoreData"):
        cha_ids = []
        db = client["youtube_data"]
        collect = db["youtube_channels_data"]
        for i in collect.find({},{"_id":0,"channel":1}):
            cha_ids.append(i["channel"]["Channel_Id"])
        if channel_id in cha_ids:
            st.success("The given channel id: " + channel_id + " already exists")
        else:
            ot=youtube_channels_data(channel_id)
            st.success(ot)

    if st.button("migrate_Data"):
        Store_Tb=all_tables()
        st.success(Store_Tb)

    show_tb=st.selectbox('Select a Table for Outlook',
                        ("Channels","Playlists","Videos","Comments"))
    if show_tb=="Channels":
        cha_table1()
    elif show_tb=="Playlists":
        playlis_table1()
    elif show_tb=="Videos":
        vid_table1()
    elif show_tb=="Comments":
        comment_table1()

elif a == "VIEW":
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        port="5432",
                        database="Youtube")
    mycursor=mydb.cursor()


    ques_sql=st.selectbox("Select Any Question",
                        ("1.What are the names of all the videos and their corresponding channels?",
                        "2.Which channels have the most number of videos, and how many videos do they have?",
                        "3.What are the top 10 most viewed videos and their respective channels?",
                        "4.How many comments were made on each video, and what are their corresponding video names?",
                        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                        "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8.What are the names of all the channels that have published videos in the year 2022?",
                        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

    if ques_sql == "1.What are the names of all the videos and their corresponding channels?":
        mycursor.execute('''select Video_Name as videosName, Channel_Name as ChannelName from videos''')
        mydb.commit()
        table1=mycursor.fetchall()
        d1=pd.DataFrame(table1,columns=["Video Name","Channel Name"])
        st.write(d1)  

        fig = px.bar(d1,x="Channel Name",y="Video Name",title="Videos & Channels Name")
        fig.update_traces(textfont_size=16,marker_color='#B70404')
        fig.update_layout(title_font_color='#DC1F1F',title_font=dict(size=20))
        st.plotly_chart(fig,use_container_width=True)

    elif ques_sql == "2.Which channels have the most number of videos, and how many videos do they have?":
        mycursor.execute('''select Channel_Name as ChannelName, Total_Videos as Total_Videos from channels order by Total_Videos desc''')
        mydb.commit()
        table2=mycursor.fetchall()
        d2=pd.DataFrame(table2,columns=["Channel Name","Video Count"])
        st.write(d2)

        fig = px.bar(d2,x="Channel Name",y="Video Count",title="Total Videos")
        fig.update_traces(textfont_size=16,marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035',title_font=dict(size=20))
        st.plotly_chart(fig,use_container_width=True)

    elif ques_sql == "3.What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute('''select Channel_Name as ChannelName ,Video_Name as VideoName , Views as views from videos where Views is not null order by Views desc limit 10''')
        mydb.commit()
        table3=mycursor.fetchall()
        d3=pd.DataFrame(table3,columns=["Channel Name","Video Name","views"])
        st.write(d3)

        fig = px.bar(d3, x="Channel Name", y="views", text="Video Name", title="Top 10 viewed videos")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)


    elif ques_sql == "4.How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute('''select Channel_Name as ChannelName,Video_Name as VideoName ,Comments as TotalComments from videos where Comments is not null ''')
        mydb.commit()
        table4=mycursor.fetchall()
        d4=pd.DataFrame(table4,columns=["Channel Name","Video Name","Comments"])
        st.write(d4)

        fig = px.bar(d4, x="Channel Name", y="Comments", text="Video Name", title="Total Comments of Each Videos")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)

    elif ques_sql == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute('''select Channel_Name as ChannelName,Video_Name as VideoName ,Likes as TotalLikes from videos where Comments is not null order by Likes desc''')
        mydb.commit()
        table5=mycursor.fetchall()
        d5=pd.DataFrame(table5,columns=["Channel Name","Video Name","Likes"])
        st.write(d5)

        fig = px.bar(d5, x="Channel Name", y="Likes", text="Video Name", title="Highest Number Of Likes")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)

    elif ques_sql == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute('''select Channel_Name as ChannelName,Video_Name as VideoName ,Likes as TotalLikes from videos''')
        mydb.commit()
        table6=mycursor.fetchall()
        d6=pd.DataFrame(table6,columns=["Channel Name","Video Name","Likes"])
        st.write(d6)

        fig = px.bar(d6, x="Channel Name", y="Likes", text="Video Name", title="Total Number Of Likes & Dislikes")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)

    elif ques_sql == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute('''select Channel_Name as ChannelName,Views as TotalViews from channels''')
        mydb.commit()
        table7=mycursor.fetchall()
        d7=pd.DataFrame(table7,columns=["Channel Name","ViewsCount"])
        st.write(d7)

        fig = px.bar(d7, x="Channel Name", y="ViewsCount", title="Total Number Of Views for Each Channel")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)

    elif ques_sql == "8.What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute('''select Channel_Name as ChannelName,Video_Name as VideoName ,Published_Date as UploadDate from videos where extract (year from Published_Date) = 2022''')
        mydb.commit()
        table8=mycursor.fetchall()
        d8=pd.DataFrame(table8,columns=["Channel Name","Video Name","Upload Date"])
        st.write(d8)

        fig = px.bar(d8, x="Channel Name", y="Upload Date",text="Video Name", title="Videos Published Date in the Year Of 2022")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)

    elif ques_sql == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute('''select Channel_Name as ChannelName,avg(Duration) as AverageDuration from videos group by Channel_Name''')
        mydb.commit()
        table9=mycursor.fetchall()
        a=pd.DataFrame(table9,columns=["Channel Name","Average Duration"])

        table9=[]
        for index,row in a.iterrows():
            chan_name = row['Channel Name']
            avg_duration = row['Average Duration']
            avg_durationstr = str(avg_duration)
            table9.append({"Channel Name":chan_name,"Time Duration":avg_durationstr})
        d9=pd.DataFrame(table9)
        st.write(d9)

        fig = px.bar(d9, x="Channel Name", y="Time Duration",title="Average Duration of All Videos ")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)

    elif ques_sql == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        mycursor.execute('''select Channel_Name as ChannelName,Video_Name as VideoName ,Comments as TOTAL_Comment from videos where Comments is not null order by Comments desc''')
        mydb.commit()
        table10=mycursor.fetchall()
        d10=pd.DataFrame(table10,columns=["Channel Name","Video Name","Total Comments"])
        st.write(d10)

        fig = px.bar(d10, x="Channel Name", y="Total Comments",text="Video Name",title="Highest Number Of Comments")
        fig.update_traces(marker_color='#9A031E')
        fig.update_layout(title_font_color='#A10035', title_font=dict(size=20))
        st.plotly_chart(fig, use_container_width=True)