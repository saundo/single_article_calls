#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
keen API calls for single_article calls

"""
from datetime import datetime
from keen.client import KeenClient

readKey = ("55cc20862508b1fae033656ba4bdb8dd0a0d71fdb6aa973c6f5856847d2e0889"
            "1236c5e79f7f51d4b3dc4d547373180758d666a1b4321e743a2cf0edfe1399f88"
            "2857b0bc3abe566f92f0c7f5fb8eda5e7cf638f8036b31b3574222f58e2f97ac2"
            "33769e271a0d4185f633821565c620")

projectID = '5605844c46f9a7307bca48aa'
keen = KeenClient(project_id=projectID, read_key=readKey)


def page_views_call(article_id, start, end):
    """return page views over time range,
    group_by: device, continent, raw referrer
    """
    event = 'read_article' 

    timeframe = {'start':start, 'end':end}

    group_by = ('glass.device', 'user.geolocation.continent',
                'raw_original_referrer')

    property_name1 = 'read.type'
    operator1 = 'eq'
    property_value1 = 'start'

    property_name2 = 'article.id'
    operator2 = 'eq'
    property_value2 = article_id
    
    filters = [{"property_name":property_name1, "operator":operator1,
                "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2,
                "property_value":property_value2}]

    t = datetime.now()

    data = keen.count(event, 
                      timeframe=timeframe,
                      group_by=group_by,
                      filters=filters)

    print(start, end, datetime.now() - t)
    return data

def uniques_call(article_id, start, end):
    """return uniques over time range,
    group_by: device, continent, raw referrer
    """
    event = 'read_article' 
    target_property = 'user.cookie.permanent.id'
    
    timeframe = {'start':start, 'end':end}

    group_by = ('glass.device', 'user.geolocation.continent',
                'raw_original_referrer')

    property_name1 = 'read.type'
    operator1 = 'eq'
    property_value1 = 'start'

    property_name2 = 'article.id'
    operator2 = 'eq'
    property_value2 = article_id
    
    filters = [{"property_name":property_name1, "operator":operator1,
                "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2,
                "property_value":property_value2}]

    t = datetime.now()

    data = keen.count_unique(event,
                             target_property=target_property,
                             timeframe=timeframe,
                             group_by=group_by,
                             filters=filters)

    print(start, end, datetime.now() - t)
    return data

def unique_time_call(article_id, start, end):
    """return sum of incremental time, grouped by cookie ids
    """
    event = 'read_article' 
    target_property = 'read.time.incremental.seconds'

    timeframe = {'start':start, 'end':end}

    group_by = 'user.cookie.permanent.id'

    property_name1 = 'article.id'
    operator1 = 'eq'
    property_value1 = article_id

    property_name2 = 'read.type'
    operator2 = 'in'
    property_value2 = [25, 50, 75, 'complete', 'tap_read_full_story']

    property_name3 = 'read.time.incremental.seconds'
    operator3 = 'gt'
    property_value3 = 0.5
    
    property_name4 = 'read.time.incremental.seconds'
    operator4 = 'lt'
    property_value4 = 400

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2},
               {"property_name":property_name3, "operator":operator3, "property_value":property_value3},
               {"property_name":property_name4, "operator":operator4, "property_value":property_value4}]

    t = datetime.now()

    data = keen.sum(event,
                    target_property=target_property,
                    timeframe=timeframe,
                    group_by=group_by,
                    filters=filters)

    print(start, end, datetime.now() - t)
    return data