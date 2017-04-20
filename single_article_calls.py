#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
single_article calls primary
"""

import pandas as pd
from thread_module import run_thread
from thread_module import read_data

from API_calls import page_views_call
from API_calls import uniques_call
from API_calls import unique_time_call

def refer_categorize(x):
    """categorizes the raw_original_referrer return; NOT EXHAUSTIVE
    """
    
    if 'facebook' in x:
        return 'facebook'
    elif 'google' in x:
        return 'google'
    elif 'flipboard' in x:
        return 'flipboard'
    elif 'linkedin' in x or 'lnkd.in' in x:
        return 'linkedin'
    elif 'qz.com' in x:
        return 'qz'
    elif x == '':
        return 'none'
    elif 't.co' in x:
        return 'twitter'
    elif 'yahoo' in x:
        return 'yahoo'
    else:
        return 'other'


def page_views(article_id, timeframe, dump_dir):
    """returns page views (read article starts) over the specified timeframe
    returns two DataFrames:
    - df_refer: page views by referrer
    - df_geo: page views by geography
    
    """

    run_thread(page_views_call, article_id, timeframe, dump_dir)
    df = read_data(dump_dir)
    df['refer'] = df['raw_original_referrer'].apply(refer_categorize)
    
    df_refer = df.pivot_table(values='result', index='refer',
                              columns='glass.device', aggfunc='sum')
    df_geo = df.pivot_table(values='result',
                            index='user.geolocation.continent',
                            columns='glass.device', aggfunc='sum')

    return df_refer, df_geo
    
    
def uniques(article_id, timeframe, dump_dir):
    """returns number of unique visitors for page views (read article starts) 
    over the specified timeframe
    returns two DataFrames:
    - df_refer_un: uniques by referrer
    - df_geo_un: uniques by geography
    """
    run_thread(uniques_call, article_id, timeframe, dump_dir)
    df = read_data(dump_dir)
    df['refer'] = df['raw_original_referrer'].apply(refer_categorize)
    
    df_refer_un = df.pivot_table(values='result', index='refer',
                              columns='glass.device', aggfunc='sum')
    df_geo_un = df.pivot_table(values='result',
                            index='user.geolocation.continent',
                            columns='glass.device', aggfunc='sum')

    return df_refer_un, df_geo_un

def unique_time(article_id, timeframe, dump_dir):
    """returns DataFrame of cookie ids and the amount of time (s) spent
    on the article id; cookie's are currently ungrouped, so duplicates 
    potentially exist
    """
    run_thread(unique_time_call, article_id, timeframe, dump_dir)
    df = read_data(dump_dir)
    
    return df
    