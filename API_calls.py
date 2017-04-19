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
    """return page views over time range, grouped by article ids
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