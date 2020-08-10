from flask import Flask, render_template, url_for, request, redirect
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from redis_handler import RedisHandler, add_toRedis

app = Flask(__name__)
redis_client = RedisHandler(exp_duration=3600*24*7)


class Lastresult():
    def __init__(self, client):
        self.nlast_postId = 'No channel/user selected'
        self.default_user = 'bourseprofile'
        self.client = client
        tposts = self.client.get_ntimes(1, 'totalPosts', 'day')
        thashtags = self.client.get_ntimes(1, 'totalHashtag', 'day')
        if len(tposts) < 1:
            self.nlast_posts = 'There is no post from the previous day'
        else:
            self.nlast_posts = tposts[-1]
        if len(thashtags) < 1:
            self.nlast_hashtag = 'There is no hashtag from the previous day'
        else:
            self.nlast_hashtag = thashtags[-1]
        self.get_defaults()

    def get_defaults(self):
        self.default_nuser = str(sum([int(x) for x in redis_client.get_ntimes(6, self.default_user, 'hour')]))
        self.default_nposts = str(sum([int(x) for x in redis_client.get_ntimes(1, 'totalPosts', 'day')]))
        self.default_nhashtags = str(sum([int(x) for x in redis_client.get_ntimes(1, 'totalHashtag', 'hour')]))
        self.default_nsymbols = str(sum([int(x) for x in redis_client.get_ntimes(1, 'totalSymbols', 'day')]))


last_result = Lastresult(redis_client)
input_list = [
    'channel_hour',
    'channel_day',
    'channel_month',
    'total_hour',
    'total_day',
    'total_month',
    'hashtag_hour',
    'hashtag_day',
    'hashtag_month'
]


@app.route('/', methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        # try:
        match_time, match_type, exists = None, None, False
        for x in input_list:
            if x in request.form.keys():
                x = x.split('_')
                match_type = x[0]
                match_time = x[1]
                exists = True
        if exists:
            if match_type == 'channel':
                key = request.form['channel']
                ntime = request.form['ntimes_channel']
                result = sum([int(x) for x in redis_client.get_ntimes(int(ntime), key, match_time)])
                last_result.nlast_postId = str(result)
                last_result.default_user = key

            elif match_type == 'total':
                ntime = request.form['ntimes_total']
                result = sum([int(x) for x in redis_client.get_ntimes(int(ntime), 'totalPosts', match_time)])
                last_result.nlast_posts = str(result)
            elif match_type == 'hashtag':
                ntime = request.form['nhashtags']
                result = sum([int(x) for x in redis_client.get_ntimes(int(ntime), 'totalHashtag', match_time)])
                last_result.nlast_hashtag = str(result)
        return redirect('/')
    else:
        last_result.get_defaults()
        return render_template('index.html', result=last_result)


@app.route('/hashtags/', methods=['GET'])
def hashtags():
    results = redis_client.get_list('hashtags', 0, -1)
    return render_template('hashtags.html', results=results)


@app.route('/posts/', methods=['GET'])
def posts():
    results = redis_client.get_list('posts', 0, -1)
    return render_template('posts.html', results=results)

if __name__ == "__main__":
    app.run(debug=True)
