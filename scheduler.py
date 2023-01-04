import datetime
import time
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
import urllib.parse
import xgboost as xgb
import os
import pandas as pd
import sqlite3
import concurrent.futures
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.impute import SimpleImputer
from category_encoders import OrdinalEncoder
from itertools import groupby
import os
import warnings
warnings.filterwarnings('ignore')



model = xgb.XGBRanker()
model.load_model(os.path.join(os.getcwd(), 'mymodel/model.model'))



def get_lastnday(date_string, n):
  date_format = '%Y%m%d'
  date = datetime.datetime.strptime(date_string, date_format)
  last_month = date - timedelta(days=n)
  last_month_string = last_month.strftime('%Y%m%d')
  return str(last_month_string)  



def get_dateData1(name, date, url, conn):
  df = 0
  alert = 0
  ll = 0
  NOR = 250
  skey1 = 'mSuMEgsxd69CVxnw0gh2mQhpq1j9WW1l2%2Beu%2FoY1xZJ56n4yXgThZk4GIrhUh6R1BhWqBKvI5Xddb%2BHrd%2FrXGA%3D%3D'
  skey2 = 'T1PytqZ5KGU3xEUwjx3NB956bZOCpETr1T6gBiY%2BhdTgfZ7sx8iW%2FNnrxh2A5iAyFva4KKkZR1VeXLXr4AXBHw%3D%3D'
  if int(date[-1])%2:
      key = skey1
  else:
      key = skey2
  while True:
    params = {
    'serviceKey' : key,
    'pageNo':'1',
    'numOfRows':str(NOR),
    'meet':'1',
    'rc_date':date
    }
    headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    query_string = urllib.parse.urlencode(params, safe="%")

    url_mod = f"{url}?{query_string}"
    f = open("abd.txt", 'w', encoding="utf8")
    f.write(url_mod)
    f.close()
    time.sleep(0.7)
    response = requests.get(url_mod, headers=headers, verify = False)
    xmlobj = BeautifulSoup(response.text.encode('utf-8'), 'lxml-xml')
    rows = xmlobj.findAll('item')
    if len(rows)==0 and 'NORMAL SERVICE' in str(response.text.encode('utf-8')):
        break
    if 'NORMAL SERVICE' not in str(response.text.encode('utf-8')):
        f = open("abc.txt", 'w', encoding="utf8")
        f.write(str(response.text.encode('utf-8')))
        f.write(f"{rows}:{date}")
        f.close()
        raise
    if len(rows)==NOR:
        NOR=round(1.5*NOR)
        continue

        
    if type(df)!=pd.core.frame.DataFrame:
        df = pd.DataFrame(columns=[i.name for i in rows[0].find_all()])
    for k in range(len(rows)):
        tm = {i.name : i.text for i in rows[k].find_all()}
        df = df.append(tm, ignore_index=True)
    break
  if type(df)==pd.core.frame.DataFrame:
    df = df.assign(MatchingPeriod=date)
    name = name.replace(' ', '_')
    df.to_sql(name, conn, if_exists='append', index=False)
  return



def get_dateData2(name, date, url, conn):
  df = 0
  alert = 0
  ll = 0
  NOR = 250
  skey1 = 'mSuMEgsxd69CVxnw0gh2mQhpq1j9WW1l2%2Beu%2FoY1xZJ56n4yXgThZk4GIrhUh6R1BhWqBKvI5Xddb%2BHrd%2FrXGA%3D%3D'
  skey2 = 'T1PytqZ5KGU3xEUwjx3NB956bZOCpETr1T6gBiY%2BhdTgfZ7sx8iW%2FNnrxh2A5iAyFva4KKkZR1VeXLXr4AXBHw%3D%3D'
  while True:
    params = {
    'serviceKey' : skey1,
    'pageNo':'1',
    'numOfRows':str(NOR),
    'meet':'1',
    'rc_date_fr':get_lastnday(date, 30),
    'rc_date_to':get_lastnday(date, 5)
    }
    headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    query_string = urllib.parse.urlencode(params, safe="%")

    url_mod = f"{url}?{query_string}"
    time.sleep(0.7)
    response = requests.get(url_mod, headers=headers, verify = False)
    xmlobj = BeautifulSoup(response.text.encode('utf-8'), 'lxml-xml')
    rows = xmlobj.findAll('item')
    if len(rows)==0 and 'NORMAL SERVICE' in str(response.text.encode('utf-8')):
        break
    if 'NORMAL SERVICE' not in str(response.text.encode('utf-8')):
        raise
    if len(rows)==NOR:
        NOR=round(1.5*NOR)
        continue

        
    if type(df)!=pd.core.frame.DataFrame:
        df = pd.DataFrame(columns=[i.name for i in rows[0].find_all()])
    for k in range(len(rows)):
        tm = {i.name : i.text for i in rows[k].find_all()}
        df = df.append(tm, ignore_index=True)
    break
  if type(df)==pd.core.frame.DataFrame:
    df = df.assign(MatchingPeriod=date)
    name = name.replace(' ', '_')
    df = df.rename(columns=lambda x: x + name[:2])
    df.to_sql(name, conn, if_exists='append', index=False)
  return


def Todo():
    DATABASE_PATH = os.path.join(os.getcwd(), 'mymodel/main.db')
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    tablelist = ['출전_등록말_정보', '경주성적정보', '기수기간별성적비교', '마주기간별성적비교', '조교사기간별전적비교','출전표_상세정보']

    endpoints = \
    {
        "기수기간별성적비교":"https://apis.data.go.kr/B551015/jkpresult/getjkpresult",
        "조교사기간별전적비교":"https://apis.data.go.kr/B551015/trpresult/gettrpresult",
        "마주기간별성적비교":"https://apis.data.go.kr/B551015/owpresult/getowpresult",
        "경주성적정보":"https://apis.data.go.kr/B551015/API214/RaceDetailResult",
        "출전_등록말_정보":"https://apis.data.go.kr/B551015/API23_1/entryRaceHorse_1",
        "출전표_상세정보":"https://apis.data.go.kr/B551015/API26_1/entrySheet_1"
    }

    for table in tablelist:
        if table == '출전_등록말_정보':
            maxMP =  pd.read_sql_query(f"SELECT MAX(MatchingPeriod)  FROM {table};", conn).to_string(index=False, header=False)
            start_date = maxMP
            end_date= datetime.datetime.today().strftime("%Y%m%d")
            dates = pd.date_range(pd.to_datetime(start_date) + pd.Timedelta(days=1), pd.to_datetime(end_date) + pd.Timedelta(days=10))
            date_strings = [d.strftime("%Y%m%d") for d in dates]
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                for date in date_strings:
                    time.sleep(0.3)
                    executor.submit(get_dateData1(table, date, endpoints[table], conn))
            MatchingPeriod = list(pd.read_sql_query(f"SELECT DISTINCT(MatchingPeriod)  FROM 출전_등록말_정보;", conn)['MatchingPeriod'])
        elif ('기간별' in table):
            maxMP1 =  pd.read_sql_query(f"SELECT MAX(MatchingPeriod{table[:2]})  FROM {table};", conn).to_string(index=False, header=False)
            cursor.execute(f'DELETE FROM {table} WHERE MatchingPeriod{table[:2]} = {maxMP1}')
            maxMP2 =  pd.read_sql_query(f"SELECT MAX(MatchingPeriod{table[:2]})  FROM {table};", conn).to_string(index=False, header=False)
            cursor.execute(f'DELETE FROM {table} WHERE MatchingPeriod{table[:2]} = {maxMP2}')
            maxMP3 =  pd.read_sql_query(f"SELECT MAX(MatchingPeriod{table[:2]})  FROM {table};", conn).to_string(index=False, header=False)
            cursor.execute(f'DELETE FROM {table} WHERE MatchingPeriod{table[:2]} = {maxMP3}')
            maxMP4 =  pd.read_sql_query(f"SELECT MAX(MatchingPeriod{table[:2]})  FROM {table};", conn).to_string(index=False, header=False)
            cursor.execute(f'DELETE FROM {table} WHERE MatchingPeriod{table[:2]} = {maxMP3}')
            maxMP5 =  pd.read_sql_query(f"SELECT MAX(MatchingPeriod{table[:2]})  FROM {table};", conn).to_string(index=False, header=False)
            cursor.execute(f'DELETE FROM {table} WHERE MatchingPeriod{table[:2]} = {maxMP3}')
            time.sleep(0.3)
            get_dateData2(table, maxMP5, endpoints[table], conn)
            get_dateData2(table, maxMP4, endpoints[table], conn)
            get_dateData2(table, maxMP3, endpoints[table], conn)
            get_dateData2(table, maxMP2, endpoints[table], conn)
            get_dateData2(table, maxMP1, endpoints[table], conn)
            end_date= datetime.datetime.today().strftime("%Y%m%d")
            MPlist = [str(p) for p in MatchingPeriod if (p > int(maxMP1)) and (p < int(end_date))]
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                for date in MPlist:
                    time.sleep(0.3)
                    executor.submit(get_dateData2(table, date, endpoints[table], conn))

        else:
            maxMP =  pd.read_sql_query(f"SELECT MAX(MatchingPeriod)  FROM {table};", conn).to_string(index=False, header=False)
            MPlist = [str(p) for p in MatchingPeriod if (p > int(maxMP)) and (p < int(end_date))]
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                for date in MPlist:
                    time.sleep(0.3)
                    executor.submit(get_dateData1(table, date, endpoints[table], conn))

    cursor.execute("SELECT * FROM totaltable LIMIT 0")
    column_names = [description[0] for description in cursor.description]
    column_names

    df = pd.read_sql_query(
    """
    Select * FROM
    (
    SELECT * FROM 출전표_상세정보[출상정]
    LEFT OUTER JOIN 출전_등록말_정보[출마정]
    ON 출상정.MatchingPeriod =출마정.MatchingPeriod AND 출상정.hrNo =출마정.hrNo
    )[출상정2]
    LEFT OUTER JOIN 경주성적정보[경성정]
    ON 출상정2.MatchingPeriod =경성정.MatchingPeriod AND 출상정2.hrNo =경성정.hrNo
    LEFT OUTER JOIN 기수기간별성적비교[기성적]
    ON 출상정2.MatchingPeriod =기성적.MatchingPeriod기수 AND 출상정2.jkNo =기성적.jkNo기수
    LEFT OUTER JOIN 마주기간별성적비교[주성적]
    ON 출상정2.MatchingPeriod =주성적.MatchingPeriod마주 AND 출상정2.owNo =주성적.owNo마주
    LEFT OUTER JOIN 조교사기간별전적비교[조성적]
    ON 출상정2.MatchingPeriod =조성적.MatchingPeriod조교 AND 출상정2.trNo =조성적.trNo조교;

    """, conn)
    column_names.remove('index')
    df = df[column_names]
    df = df.replace(to_replace=['', None], value=pd.np.nan)
    df = df = df.replace(to_replace='^[A-Z0-9]{6}$', value=pd.np.nan, regex=True)
    for col in df.select_dtypes(include='object').columns:
        try:
            df[col] = df[col].astype(float)
        except:
            pass
    object_columns = df.select_dtypes(include='object')
    object_columns = object_columns.drop(columns=['sex'])
    df = df.drop(columns=object_columns.columns)
    df = df.T.drop_duplicates(keep='first').T
    for col in df.select_dtypes(include='object').columns:
        try:
            df[col] = df[col].astype(float)
        except:
            pass
    OrdinalMap = [
                {'col': 'sex', 
                'mapping': {'수': 3, '거': 2, '암': 1}}
                ]
    pipe = make_pipeline(
        OrdinalEncoder(mapping = OrdinalMap),
        SimpleImputer(strategy='most_frequent')
    )


    df_data = pipe.fit_transform(df)
    df = pd.DataFrame(df_data, columns = df.columns)
    df = df.rename(columns={'rcDate': 'MatchingPeriod'})
    df['MatchingPeriod'] = df['MatchingPeriod'].astype(int)
    df.to_sql('totaltable', conn, if_exists='replace')


    X_df=df.drop(['MatchingPeriod', 'ord', 'rcNo'], axis=1)
    y_df = df['ord']
    g_df1=df.apply(lambda row: str(int(row['MatchingPeriod'])) + format(int(row['rcNo']), '02d'), axis=1)
    g_df=[len(list(group)) for key, group in groupby(g_df1)]

    predict = model.predict(X_df,g_df)
    result = pd.DataFrame({'y': y_df, 'predict': predict, 'g' : g_df1})

    result = result.assign(predict2 = result.groupby('g')['predict'].rank())

    result = pd.concat([result, df[['MatchingPeriod','rcNo', 'hrNo']]], axis = 1)


    result.to_sql('resulttable', conn, if_exists='replace')
    conn.close()
    return