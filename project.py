#importing the required libraries
import googleapiclient.discovery
from pprint import pprint
import pandas as pd
import mysql.connector
import sqlite3
import streamlit as st
# channel_id='UCDfCAienRzwXYa2HrfQmdhg'
api_key="AIzaSyAut0YqE5oyWWfCovbExlxXizhAu-2yyLI"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)
#scraping information from youtube
def channel_info(channel_id):
    chan_info=[]
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id)
    channel_data = request.execute()
    channel_informations = {
        'channel_name' : channel_data['items'][0]['snippet']['title'],
        'channel_description' : channel_data['items'][0]['snippet']['description'],
        'total_videos':channel_data['items'][0]['statistics']['videoCount'],
        'playlists' : channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        "channel_id": channel_data['items'][0]['id'],
        "subscription_count": channel_data['items'][0]['statistics']['subscriberCount'],
        "channel_view":channel_data['items'][0]['statistics']['viewCount']}
    chan_info.append(channel_informations)
    
    return chan_info


def video_id(channel_id): 
    v_id=[]
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id)
    channel_data=request.execute()
    next_page_Token=None
    playlistid=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    try:
        while True :
            request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50,
            playlistId=playlistid,
            pageToken=next_page_Token)
            play_listinfo = request.execute()
            # pprint( play_listinfo['items'])
            
            for i in play_listinfo['items']:
                v_id.append(i['contentDetails']['videoId'])
            next_page_Token=play_listinfo.get('nextPageToken')
            # pprint(v_id)

            if next_page_Token is None:
                break
    except:
          print("no videoid found" )
        
    return v_id 


def video_information(v_id):
    vi_info=[]
    for i in v_id:
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        
        id=i)
        video = request.execute()
        for item in video['items']:
            s=item['contentDetails']['duration']
            l=[]
            f=''
            for i in s:
                if i.isnumeric():
                    f=f+i
                else:
                    if f:
                        l.append(f)
                        f=''
            if "H"  not in s:
                l.insert(0,'00')
            if "M" not in s:
                l.insert(1,'00')
            if "S" not in s:
                l.insert(-1,'00')
        duration=':'.join(l)                                
        video_info={"videoid":item['id'],
                             'channel_id':item['snippet']['channelId'],
                             'channel_name':item['snippet']['channelTitle'],
                            'vidoe_description':item['snippet']['description'],
                            'video_title':item['snippet']['title'],
                            'published_date':item['snippet']['publishedAt'],
                            'thumbnail':item['snippet']['thumbnails']['default']['url'],
                            'comment_count':item['statistics'].get('commentCount'),
                            'like_count':item['statistics'].get('likeCount'),
                            'dislike_count':item['statistics'].get('dislikeCount'),
                            'view_count':item['statistics'].get('viewCount'),
                            'favourite_count':item['statistics'].get('favoriteCount'),
                            'duration':duration,
                            'caption_status':item['contentDetails']['caption']}
        vi_info.append(video_info)
       
    return(vi_info)



def comment1(v_id):

        comment_info=[]
        try:
            for i in v_id:
                request = youtube.commentThreads().list(
                part="snippet,replies",
                
                videoId=i)
           
                comment_in = request.execute()
                for j in comment_in['items']:
                      
                      info={
                      'video_id':j['snippet']['topLevelComment']['snippet']['videoId'] , 
                      'comment_id':j['id'],    
                      'comment_text':j['snippet']['topLevelComment']['snippet']['textDisplay'],
                      'comment_author':j['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                       'comment_published_at':j['snippet']['topLevelComment']['snippet']['publishedAt']}
                      comment_info.append(info)
        except:
            pass 
                           
        return comment_info   
    
                            
def play_list_info(channel_id):
    next_page_token=None
    lister=[]
    while True:
        request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25,
        pageToken=next_page_token)
        playlist=request.execute()
        for item in playlist['items']:
            info={'play_list_id':item['id'],
                  'title':item['snippet']['title'],
                  'channelid':item['snippet']['channelId'],
                  'channel_name':item['snippet']['channelTitle']}

            lister.append(info)
        next_page_Token=playlist.get('nextPageToken')
        if next_page_Token is None:
            break
    
    return lister

#connecting mongodb
from pymongo.mongo_client import MongoClient
uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db=client["youtube"]
#inserting data into mongodb
def project(channel_id):
    channeldata=channel_info(channel_id)
    videodetail=video_id(channel_id)
    videoinfo=video_information(videodetail)
    comment=comment1(videodetail) 
    playlist=play_list_info(channel_id)
    info={'channeldetail':channeldata,
          'videoinfo':videoinfo,
          'commentinfo':comment,
           'playlistinfo':playlist}
    db=client["youtube"]
    col=db['channel']
    col.insert_one(info)
    return  

#connection to mysql
config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'youtube'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
#creating tables
def channeltable():
    config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'youtube'}

    print(config)
    connection = mysql.connector.connect(**config)
    cursor=connection.cursor()
    print(cursor)
    drop_query="""DROP TABLE if exists channelinfo"""
    cursor.execute(drop_query)
    connection.commit()
    create_table= """CREATE TABLE if not exists channelinfo (channel_name VARCHAR(100),channel_description TEXT,total_videos BIGINT,
    playlists VARCHAR(100),channel_id VARCHAR(255) PRIMARY KEY,subscription_count INT,channel_view INT)"""
    cursor.execute(create_table)
    connection.commit()
    db=client["youtube"]
    col=db['channel']

    for channeldetail1 in col.find({},{"channeldetail":1}):
        
        chdes=channeldetail1['channeldetail'][0]['channel_description']
        ch=channeldetail1['channeldetail'][0]['channel_name']
        no=channeldetail1['channeldetail'][0]['total_videos']
        play=channeldetail1['channeldetail'][0]['playlists']
        id=channeldetail1['channeldetail'][0]['channel_id']
        subcount=channeldetail1['channeldetail'][0]['subscription_count']
        views=channeldetail1['channeldetail'][0]['channel_view']
        Insert_Query = '''Insert into channelinfo(channel_name,channel_description,total_videos,playlists ,
        channel_id ,subscription_count,channel_view) values(%s, %s,%s,%s,%s,%s,%s);'''
        data_to_insert=(ch,chdes,no,play,id,subcount,views)
        cursor.execute(Insert_Query,data_to_insert)
    connection.commit()
    return


def playlisttable():
    config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'youtube'}

    connection = mysql.connector.connect(**config)
    cursor=connection.cursor()
    print(cursor)

    drop_query="""DROP TABLE if exists playlistinfo"""
    cursor.execute(drop_query)
    connection.commit()


    create_table= """CREATE TABLE if not exists playlistinfo (play_list_id VARCHAR(100),title VARCHAR(100),
    channelid VARCHAR(255),channel_name VARCHAR(255))"""
    cursor.execute(create_table)
    connection.commit()


    
    db=client["youtube"]
    col=db['channel']
    info=col.find()
    lister=[]
    for i in info:
        for j in i['playlistinfo']:
            lister.append(j)


    
    from sqlalchemy import create_engine
    con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/youtube")
    playlistdetail=pd.DataFrame(lister)


    oldplaydetail= pd.read_sql_query("SELECT * FROM playlistinfo", con)
    newplaydetail=pd.concat([oldplaydetail,playlistdetail ], ignore_index=True)
    newplaydetail.drop_duplicates(inplace=True)
    newplaydetail.to_sql('playlistinfo', con, if_exists='replace', index=False)

    return



def commenttable():
    config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'youtube'}

    connection = mysql.connector.connect(**config)
    cursor=connection.cursor()
    print(cursor)

    drop_query="""DROP TABLE if exists commentinfo"""
    cursor.execute(drop_query)
    connection.commit()

    create_table= """CREATE TABLE if not exists commentinfo(video_id VARCHAR(100),comment_id VARCHAR(100),
    comment_text TEXT,comment_author VARCHAR(255),comment_published_at TIMESTAMP)"""
    cursor.execute(create_table)
    connection.commit()

    client = MongoClient(uri)
    db=client["youtube"]
    col=db['channel']
    info=col.find()
    lister=[]
    for i in info:
        for j in i['commentinfo']:
            lister.append(j)
    from sqlalchemy import create_engine
    con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/youtube")
    commentdetail=pd.DataFrame(lister)
    oldcommentdetail= pd.read_sql_query("SELECT * FROM commentinfo", con)
    newcommentdetail=pd.concat([oldcommentdetail,commentdetail ], ignore_index=True)
    newcommentdetail.drop_duplicates(inplace=True)
    newcommentdetail.to_sql('commentinfo', con, if_exists='replace', index=False)
    
    return


def videotable():
    config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'youtube'}

    connection = mysql.connector.connect(**config)
    cursor=connection.cursor()
    print(cursor)

    drop_query="""DROP TABLE if exists videoinfo"""
    cursor.execute(drop_query)
    connection.commit()

    create_table= """CREATE TABLE if not exists videoinfo(videoid VARCHAR(100),channel_id VARCHAR(100),channel_name VARCHAR(100),
    vidoe_description TEXT,video_title VARCHAR(255),published_date TIMESTAMP,thumbnail VARCHAR (255),comment_count  BIGINT,like_count  BIGINT,
    dislike_count  BIGINT,view_count BIGINT,favourite_count BIGINT,duration TIME,caption_status VARCHAR(50)) """
    cursor.execute(create_table)
    connection.commit()

    client = MongoClient(uri)
    db=client["youtube"]
    col=db['channel']
    info=col.find()
    lister=[]
    for i in info:
        for j in i['videoinfo']:
            lister.append(j)
    from sqlalchemy import create_engine
    con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/youtube")
    videodetail=pd.DataFrame(lister)


    oldvideodetail= pd.read_sql_query("SELECT * FROM videoinfo", con)
    newvideodetail=pd.concat([oldvideodetail,videodetail ], ignore_index=True)
    newvideodetail.drop_duplicates(inplace=True)
    newvideodetail.to_sql('videoinfo', con, if_exists='replace', index=False)
    return

      

def tables():
    channeltable()
    playlisttable()
    commenttable()
    videotable()
    return 
#functions to show the particular table in streamlit  

def showchanneltable():
    lister1=[]
    client = MongoClient(uri)
    db=client["youtube"]
    col=db['channel']
    for data in col.find({},{"_id":0,"channeldetail":1}):
        lister=data['channeldetail'][0]
        lister1.append(lister)
    df1=st.dataframe(lister1)
    return df1
  
def showplaylisttable():    
    client = MongoClient(uri)
    db=client["youtube"]
    col=db['channel']
    info=col.find()
    lister=[]
    for i in info:
        for j in i['playlistinfo']:
            lister.append(j)
    df2=st.dataframe(lister) 
    return df2

def showcommenttable():   
    client = MongoClient(uri)
    db=client["youtube"]
    col=db['channel']
    info=col.find()
    lister=[]
    for i in info:
        for j in i['commentinfo']:
            lister.append(j)
    df3=st.dataframe(lister)
    return df3

def showvideotable():    
    client = MongoClient(uri)
    db=client["youtube"]
    col=db['channel']
    info=col.find()
    lister=[]
    for i in info:
        for j in i['videoinfo']:
            lister.append(j)
    df4=st.dataframe(lister)
    return  df4      
#program to show the information to streamlit
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header('project detail')
    st.caption("migration of data into sql")
    st.caption("mongodb database")
    st.caption("mysql database")
channel_id=st.text_input("Enter the channel Id :") 
if st.button("data retrival in mongodb"):
    channel_ids=[]
    client = MongoClient(uri)
    db=client["youtube"]
    col=db['channel']
    for data in col.find({},{"_id":0,"channeldetail":1}):
        channel_ids.append(data["channeldetail"][0]['channel_id'])
    
    if channel_id in channel_ids:
        st.success("channel information already exists")
    else:
        insert=project(channel_id)
        st.success(insert)       
        
if st.button("migrate to sql"):
    tablesinfo=tables()
    st.success(tablesinfo)

display_table=st.radio("Select the table for view",("channel","playlist","videos","comments"))
if display_table== "channel":
    showchanneltable()

elif display_table=="playlist":
    showplaylisttable()
elif display_table=="videos":
    showvideotable()
elif display_table=="comments":
    showcommenttable()       
#fetching information from mysql using query   
import mysql.connector
config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'youtube'}

print(config)


connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)

Questions=st.selectbox("Select your questions",("1.Name of the video and their channel name",
                                                "2.Channel with most no. of videos and their number no. of videos",
                                                "3.Top 10 most viewed videos and their channel",
                                                "4.Number of comment in each video and their names",
                                                "5.The highest number of likes and channel name",
                                                "6.The number of like and dislike for each video and corresponding name",
                                                "7.The number of view for each channel and their name",
                                                "8.The name of the channel that are published in the year 2022",
                                                "9.The duration of all videos in a channel and their channel name",
                                                "10.The highest number of comment and their channel name"))
import mysql.connector
config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'youtube'}

connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)

if Questions=="1.Name of the video and their channel name":
    query1='''select video_title as title,channel_name as channelname from videoinfo'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df1)


elif Questions=="2.Channel with most no. of videos and their number no. of videos":
    query2='''Select channel_name as channelname,total_videos as no_videos from channelinfo order by total_videos desc'''
    cursor.execute(query2)
   
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channelname",'no_videos'])
    st.write(df2)

elif  Questions=="3.Top 10 most viewed videos and their channel":
    query3='''Select view_count as view_count,channel_name as channelname,video_title as videotitle from videoinfo 
    where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query3)
    
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views",'channel name',"video title"])
    st.write(df3)  

elif  Questions=="4.Number of comment in each video and their names":
    query4='''Select comment_count as comments,video_title as videotitle from videoinfo 
    where comment_count is not null'''
    cursor.execute(query4)

    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comment","video title"])
    st.write(df4)      
elif  Questions=="5.The highest number of likes and channel name":
    query5='''Select channel_name as channelname,video_title as videotitle,like_count as no_likes from videoinfo 
            where  like_count is not null order by like_count desc'''
    cursor.execute(query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["channel name","video title","like count"])
    st.write(df5)    

elif  Questions=="6.The number of like and dislike for each video and corresponding name":
    query6="""Select dislike_count as no_dislikes ,video_title as videotitle,like_count as no_likes from videoinfo"""
    cursor.execute(query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["dislike count","video title","like count"])
    st.write(df6)  

elif  Questions=="7.The number of view for each channel and their name":
    query7="""Select channel_name as channelname ,channel_view as channelview from channelinfo"""
    cursor.execute(query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","channel view"])
    st.write(df7) 

elif  Questions=="8.The name of the channel that are published in the year 2022":
    query8="""SELECT video_title as videotitle,published_date as videorelease,channel_name as channelname FROM videoinfo WHERE extract(year from published_date)=2022"""
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","published date","channel name"])
    st.write(df8)    

elif  Questions=="9.The duration of all videos in a channel and their channel name":
    query9="""SELECT channel_name as channelname,SEC_TO_TIME(AVG(TIME_TO_SEC(duration))) as averageduration FROM videoinfo group by channel_name"""
    cursor.execute(query9)
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channel_name","average_duration"])

    t9=[]
    for index,rows in df9.iterrows():
        channel_title=rows["channel_name"]
        average_duration=rows["average_duration"]
        average_duration_str=str( average_duration)
        t9.append(dict(channeltitle=channel_title,averageduration=average_duration_str))
    df=pd.DataFrame(t9) 
    st.write(df)   
elif  Questions=="10.The highest number of comment and their channel name":
    query10="""SELECT channel_name as channelname,comment_count as commentcount ,video_title as videoname FROM videoinfo
      WHERE comment_count is not null order by comment_count desc"""
    cursor.execute(query10)
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["channel name","commentcount","video name"])
    st.write(df10) 
