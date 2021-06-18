"""This service allows to update videos to db"""
import os
import sys
import time
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor

r = get_redis()

def update_videos(data):
    """Updates A SINGLE VIDEO in database (table videos)
       data must be a 1d array - [13]"""
    cursor, db = get_cursor()
    if not cursor or not db:
        # log that failed getting cursor
        return False
    try:
        if data is None or len(data) != 13:
            return False
        q = f"SELECT id, views, likes, dislikes, comments FROM videos WHERE id = '{data[0]}'"
        cursor.execute(q)
        data2 = cursor.fetchall()
        if data == ():
            # log video isn't in db
            return False
        q = '''REPLACE INTO videos
                (id, title, views, likes,
                dislikes, comments, description,
                channel_id, duration, published_at,
                tags, default_language, made_for_kids, time)
                VALUES
                (%s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, NOW() );'''
        cursor.execute(q, data)
        q = '''INSERT INTO videos_history
                (video_id, views, likes,
                dislikes, comments, update_time)
                VALUES
                (%s, %s, %s, %s, %s, NOW() );'''
        cursor.execute(q, data2[0])
        db.commit()
    except Exception as error:
        print(error)
        # LOG
        return False
        # sys.exit("Error:Failed writing new videos to db")

    return True


if __name__ == '__main__':
    q = Queue('update_videos', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='update_videos')
        worker.work()
