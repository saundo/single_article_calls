#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
single_article calls primary
"""

from thread_module import run_thread
from thread_module import read_data

from API_calls import page_views_call


def page_views(article_id, timeframe, dump_dir):
    """returns page views (read article starts) over the specified timeframe
    returns two DataFrames:
    - df_refer: page views by referrer
    - df_geo: page views by geography
    
    """

    run_thread(page_views_call, timeframe, dump_dir)
    z = read_data(dump_dir)
    
    
