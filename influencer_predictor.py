import requests
import os
import random
import pickle
import unicodedata
import re
import string
from collections import Counter
from random import randint, choice, sample
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, and_, or_, Boolean, PickleType, DateTime, Float, BigInteger
from sqlalchemy.sql import select, func, text, update
from sqlalchemy.sql.expression import bindparam
import gc
import pandas
import statsmodels.api as sm


engine = create_engine('mysql+pymysql://admin:SI04HKRXtUL3HKlgY9yo@main-db.cyvqrdlmywtq.us-east-1.rds.amazonaws.com/insta?charset=utf8mb4', pool_pre_ping=True)
engine_crm = create_engine('mysql+pymysql://admin:yqvKDQ62NgBbvh2UysyJ@crm-db.cyvqrdlmywtq.us-east-1.rds.amazonaws.com/crm_data?charset=utf8mb4', pool_pre_ping=True)

meta = MetaData()

data_cols = ["insta_id","nr_followers", "nr_followings", "nr_medias", "ER", "cleaned_ER", "recurring_ER", "comment_rate", "crawled", "category", "gender", "ethnicity", "age", "is_human_inprofile", "perc_humanposts", "perc_englcomments", "comms_median_engl_word", "post_per_week", "avg_daily_stories", "perc_foodpics", "perc_replied_comments_infl", "perc_replied_comments_all", "avg_hashtags", "is_author", "is_blogger","likers_6","likers_8","likers_10","likers_12"]

influencer_analytics = Table(
   'influencer_analytics', meta,
   Column('insta_id', BigInteger, primary_key = True), 
   Column('username', String(100)), 
   Column('nr_followers', Integer), 
   Column('nr_followings', Integer), 
   Column('nr_medias', Integer),
   Column('infos', PickleType), 
   Column('account_type', Integer),
   Column('is_private', Boolean),
   Column('ER', Float),
   Column('cleaned_ER', Float),
   Column('recurring_ER', Float),
   Column('C2L', Float),
   Column("comment_rate", Float),
   Column('followings', PickleType),
   Column('followers', PickleType),
   Column('likes', PickleType), 
   Column('all_comments', PickleType), 
   Column('crawled', Integer),
   Column('email', String(200)),
   Column('country', String(200)),
   Column('category', String(200)),
   Column('pic_url', String(1000)),
   Column('gender', Integer), # 0 = unkown, 1 = Male, 2 = Female
   Column('ethnicity', String(100)),
   Column('age', Integer),
   Column('is_human_inprofile', Boolean),
   Column('perc_humanposts', Float),
   Column('perc_englcomments', Float),
   Column('comms_median_engl_word', Integer),
   Column('reason_score', Integer),
   Column('post_per_week', Float),
   Column('avg_daily_stories', Float),
   Column('food_followings', Float),
   Column('perc_foodpics', Float),
   Column('perc_replied_comments_infl', Float),
   Column('perc_replied_comments_all', Float),
   Column('avg_hashtags', Float),
   Column('pics_aesthetics', Float),
   Column('pics_tech_aesth', Float),
   Column('is_author', Boolean),
   Column('is_blogger', Boolean),
   Column('has_profile_pic', Boolean),
   Column('is_personal', Boolean),
   Column('likers_6', Integer),
   Column('likers_8', Integer),
   Column('likers_10', Integer),
   Column('likers_12', Integer),
   )

deal_db = Table(
   'deal_db', meta,
   Column('deal_id', Integer, primary_key = True), 
   Column('insta_id', BigInteger), 
   Column('handle', String(200)),
   Column('funnel_step', Integer), 
   Column('perf_clicks', Integer), 
   Column('perf_referrals', Integer), 
   Column('perf_sales', Integer), 
   )

outreach_db = Table(
   'outreach_db', meta,
   Column('insta_id', BigInteger, primary_key = True), 
   Column('predict_clicks', Integer), 
   Column('is_mom', Integer), 
   )

upd_stmt_outreach_preds = outreach_db.update().\
    where(outreach_db.c.insta_id == bindparam('_insta_id')).\
    values({
        'predict_clicks': bindparam('_predict_clicks'),
    })




def get_deals():
    slc_stmt = select([deal_db]).where(deal_db.c.funnel_step >=4)
    res = crm_conn.execute(slc_stmt).fetchall()
    influencer_ids = [d[1] for d in res]
    forbidden_deals = [233, 252,277]
    df = pandas.DataFrame(res,columns=["deal_id","insta_id","handle","funnel_step","clicks","referrals","sales"])
    df = df[~df["deal_id"].isin(forbidden_deals)]
    df = df.fillna(0)
    slc_stmt = select([influencer_analytics.c.insta_id, influencer_analytics.c.post_per_week, influencer_analytics.c.nr_followers , 
                        influencer_analytics.c.avg_hashtags, influencer_analytics.c.ER, ]).where(influencer_analytics.c.insta_id.in_(influencer_ids))
    res_infl = insta_conn.execute(slc_stmt).fetchall()
    colnames = insta_conn.execute(slc_stmt).keys()
    res_dicts = []
    for infl in res_infl:
        res_dicts.append({col_n:infl[i] for i, col_n in enumerate(colnames) if col_n in data_cols})
    analytics = pandas.DataFrame(res_dicts)
    df = df.merge(analytics, on="insta_id").set_index(keys=["insta_id"])
    df["nr_likes"] = df["ER"] * df["nr_followers"]
    df["hashtag_over_15"] = (df["avg_hashtags"]>15).astype(int)
    return df

def get_model(df):
    variables = ['post_per_week', "nr_likes","hashtag_over_15"]
    new_df = df[variables+["clicks"]].dropna()
    X, y = new_df[variables], new_df["clicks"]
    est = sm.OLS(y, X)
    est2 = est.fit()
    print(est2.summary())
    return est2

def get_df_to_predict():
    slc_stmt = select([outreach_db.c.insta_id])
    res = crm_conn.execute(slc_stmt).fetchall()
    influencer_ids = [d[0] for d in res]
    slc_stmt = select([influencer_analytics.c.insta_id, influencer_analytics.c.post_per_week, influencer_analytics.c.nr_followers , 
                        influencer_analytics.c.avg_hashtags, influencer_analytics.c.ER, ]).where(influencer_analytics.c.insta_id.in_(influencer_ids))
    res_infl_q = insta_conn.execute(slc_stmt)
    res_infl = res_infl_q.fetchall()
    colnames = res_infl_q.keys()
    res_dicts = []
    for infl in res_infl:
        res_dicts.append({col_n:infl[i] for i, col_n in enumerate(colnames) if col_n in data_cols})
    df = pandas.DataFrame(res_dicts)
    df["nr_likes"] = df["ER"] * df["nr_followers"]
    df["hashtag_over_15"] = (df["avg_hashtags"]>15).astype(int)
    return df

def insert_predictions(model, pred_df):
    pred_df["preds"] = model.predict(pred_df[['post_per_week', "nr_likes","hashtag_over_15"]])
    pp_list = pred_df.dropna().to_dict('records')
    all_infl = [{"_insta_id":p["insta_id"], "_predict_clicks":p["preds"],} for p in pp_list]
    res = crm_conn.execute(upd_stmt_outreach_preds, all_infl)    
    

crm_conn = engine_crm.connect()
insta_conn = engine.connect()


def main():
    df = get_deals()
    model = get_model(df)
    pred_df = get_df_to_predict()
    insert_predictions(model, pred_df)
    
    insta_conn.close()
    crm_conn.close()

main()