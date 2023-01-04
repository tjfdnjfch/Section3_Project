import os
from flask import Flask, render_template, request, jsonify, Response, redirect
import xgboost as xgb
from calendar import Calendar
import sqlite3
import pandas as pd

import time
from apscheduler.schedulers.background import BackgroundScheduler

import configparser

import scheduler
from datetime import datetime
from itertools import groupby
import json



model = xgb.XGBRanker()
model.load_model(os.path.join(os.getcwd(), 'mymodel/model.model'))


sched = BackgroundScheduler()

@sched.scheduled_job('interval', minutes=30, id='updateDB')
def schedule():
    scheduler.Todo()

sched.start()




app = Flask(__name__)








app.config['mymodel'] = os.path.join(os.path.dirname(__file__), 'mymodel/model.model')



conn = sqlite3.connect(os.path.join(os.getcwd(), 'mymodel/main.db'), check_same_thread=False)









model = xgb.XGBRanker()
model.load_model(app.config['mymodel'])





rcevents = pd.read_sql_query(f"SELECT DISTINCT MatchingPeriod, rcNo  FROM 출전표_상세정보;", conn)
Etitles=rcevents.apply(lambda row:f"제{'{:02d}'.format(row['rcNo'])}경기", axis=1)
Edates=rcevents.apply(lambda row:datetime.strptime(str(row['MatchingPeriod']), '%Y%m%d').strftime('%Y-%m-%d'), axis=1)
Eurls=rcevents.apply(lambda row:f"/event_detail/{row['MatchingPeriod']}/{row['rcNo']}", axis=1)
events = [{'title': a, 'date': b, 'url':c} for (a,b,c) in zip(Etitles, Edates, Eurls)]




app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

@app.route('/')
@app.route('/calendar')
def calendar():
    return render_template('calendar.html', events=events)

@app.route('/event')
def jsonapi():
    date = request.args.get('date')
    rcNo = request.args.get('rcNo')
    df = \
    pd.read_sql_query(
        f"""SELECT *  FROM totaltable
        WHERE MatchingPeriod = {date}
        AND rcNo = {rcNo};
        """, conn)
    df = df.drop(['index'], axis=1)

    X_df=df.drop(['MatchingPeriod', 'ord', 'rcNo'], axis=1)
    y_df = df['ord']
    g_df1=df.apply(lambda row: str(int(row['MatchingPeriod'])) + format(int(row['rcNo']), '02d'), axis=1)
    g_df=[len(list(group)) for key, group in groupby(g_df1)]
    predict = model.predict(X_df,g_df)
    result = pd.DataFrame({'predict': predict, 'g' : g_df1})
    result = result.assign(predict2 = result.groupby('g')['predict'].rank())
    result = result[['predict2']].astype(int)

    hrname = pd.read_sql_query(
    f"""
    SELECT DISTINCT TT.hrNo, 출마정.hrName FROM totaltable[TT]
    LEFT OUTER JOIN 출전_등록말_정보[출마정]
    ON TT.hrNo= 출마정.hrNo
    WHERE TT.MatchingPeriod = {date}
    AND TT.rcNo = {rcNo};
    """, conn)
    hrname['hrNo']=hrname['hrNo'].astype(int)
    df_concat = pd.concat([result, hrname], ignore_index=True, axis=1)
    df_concat.columns = ['예상등수','마번','마명']



    json_data = json.dumps(df_concat.to_dict(orient='records'), indent=4, ensure_ascii=False)


    return Response(json_data, mimetype='application/json')


@app.route('/event_detail/<int:date>/<int:rcNo>')
def event_detail(date, rcNo):

    return redirect(f'http://localhost:3000/public/dashboard/b23459f7-baca-4b22-8be1-93d67d08cea3?date={date}&rcno={rcNo}')


if __name__ == '__main__':
    app.run()

