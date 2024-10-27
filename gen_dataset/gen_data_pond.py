import pandas as pd
import json 
import ast 
import duckdb
import uuid
from datetime import datetime, timedelta 
import random
import glob
import re
import warnings
warnings.filterwarnings('ignore')

def filter_swamp_water(swamp_water):
    swamp_water.rename(columns={'fullVisitorId': 'user_id', 'socialEngagementType':'user_social_engagement', 'customDimensions':'user_region','visitId':'session_id','visitStartTime':'session_start_time','hits':'session_events','date':'session_date','channelGrouping':'session_marketing_channel','visitNumber':'session_sequence_number'}, inplace=True)
    swamp_water['session_date'] = pd.to_datetime(swamp_water['session_date'], format='%Y%m%d')
    swamp_water['session_date'] = swamp_water['session_date'].dt.strftime('%Y-%m-%d')
    swamp_water['device'] = swamp_water['device'].apply(lambda x: json.loads(x))
    swamp_water['session_browser'] = swamp_water['device'].apply(lambda x: x['browser'])
    swamp_water['session_os'] = swamp_water['device'].apply(lambda x: x['operatingSystem'])
    swamp_water['session_is_mobile'] = swamp_water['device'].apply(lambda x: x['isMobile'])
    swamp_water['session_device_category'] = swamp_water['device'].apply(lambda x: x['deviceCategory'])
    swamp_water['geoNetwork'] = swamp_water['geoNetwork'].apply(lambda x: json.loads(x))
    swamp_water['session_country'] = swamp_water['geoNetwork'].apply(lambda x: x['country'])
    swamp_water['session_city'] = swamp_water['geoNetwork'].apply(lambda x: x['city'])
    swamp_water['session_region'] = swamp_water['geoNetwork'].apply(lambda x: x['region'])
    swamp_water['trafficSource'] = swamp_water['trafficSource'].apply(lambda x: json.loads(x))
    swamp_water['session_source'] = swamp_water['trafficSource'].apply(lambda x: x['source'])
    swamp_water['session_medium'] = swamp_water['trafficSource'].apply(lambda x: x['medium'])
    swamp_water['totals'] = swamp_water['totals'].apply(lambda x: json.loads(x))
    swamp_water['session_revenue'] = swamp_water['totals'].apply(lambda x: (int(x['transactionRevenue'])/1000000) if 'transactionRevenue' in x else 0)
    swamp_water['session_total_revenue'] = swamp_water['totals'].apply(lambda x: (int(x['totalTransactionRevenue'])/1000000) if 'totalTransactionRevenue' in x else 0)
    swamp_water['session_order_cnt'] = swamp_water['totals'].apply(lambda x: int(x['transactions']) if 'transactions' in x else 0)
    swamp_water['session_pageview_cnt'] = swamp_water['totals'].apply(lambda x: int(x['pageviews']) if 'pageviews' in x else 0) 
    swamp_water['session_duration'] = swamp_water['totals'].apply(lambda x: int(x['timeOnSite']) if 'timeOnSite' in x else 0)
    swamp_water['session_is_first_visit'] = swamp_water['totals'].apply(lambda x: (int(x['newVisits'])==1) if 'newVisits' in x else False)
    swamp_water.drop(columns=['device','geoNetwork','totals','trafficSource'], inplace=True)
    swamp_water.loc[swamp_water['session_city'].str.contains('available'), 'session_city'] = None
    swamp_water.loc[swamp_water['session_region'].str.contains('available'), 'session_region'] = None
    swamp_water['session_medium'] = swamp_water['session_medium'].replace('(none)', None)
    swamp_water['session_source'] = swamp_water['session_source'].replace('(direct)', 'direct')
    swamp_water['session_events'] = swamp_water['session_events'].apply(lambda x: ast.literal_eval(x))
    swamp_water['session_landing_screen'] = swamp_water['session_events'].apply(lambda x: {p['appInfo']['landingScreenName'] for p in x if 'appInfo' in p})
    swamp_water['session_landing_screen'] = swamp_water['session_landing_screen'].apply(lambda x: list(x)[0] if len(x) > 0 else None)
    swamp_water['session_exit_screen'] = swamp_water['session_events'].apply(lambda x: {p['appInfo']['exitScreenName'] for p in x if 'appInfo' in p})
    swamp_water['session_exit_screen'] = swamp_water['session_exit_screen'].apply(lambda x: list(x)[0] if len(x) > 0 else None) 
    return swamp_water

def filter_keys(event_list):
    keys_to_keep = ['hour', 'minute', 'page', 'appInfo', 'product']
    return [{key: event[key] for key in keys_to_keep if key in event} for event in event_list]

def construct_pageview_event_list(events, session_date):
    pageview_events = []
    for event in events:
        pageview_event = {}
        timestamp = pd.to_datetime(str(session_date) + ' ' + str(event['hour']) + ':' + str(event['minute']) + ':00', format='%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        pageview_event['pageview_timestamp'] = timestamp
        for key in ['hostname', 'pagePath', 'pageTitle', 'pagePathLevel1', 'pagePathLevel2', 'pagePathLevel3', 'pagePathLevel4']:
            pageview_event[key] = event.get('page', {}).get(key, None)
        pageview_event['total_product_impressions'] = len(event.get('product', []))
        pageview_events.append(pageview_event)
    return pageview_events

def adjust_timestamps(pageview_list):
    if len(pageview_list) <= 1:
        return pageview_list
    current_timestamp = datetime.strptime(pageview_list[0]['pageview_timestamp'], '%Y-%m-%d %H:%M:%S')
    for i in range(1, len(pageview_list)):
        minutes_increment = random.randint(1, 29)
        current_timestamp += timedelta(minutes=minutes_increment)
        pageview_list[i]['pageview_timestamp'] = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return pageview_list

def explode_pageviews(df):
    db = duckdb.connect(database=':memory:', read_only=False)
    db.register('session_pvs', df)
    qry = """
        SELECT 
            sp.user_id,
            sp.session_id,
            sp.session_start_time,
            pv.pageview_timestamp,
            pv.hostname,
            pv.pagePath,
            pv.pageTitle,
            pv.pagePathLevel1,
            pv.pagePathLevel2,
            pv.pagePathLevel3,
            pv.pagePathLevel4,
            pv.total_product_impressions
        FROM session_pvs AS sp,
            UNNEST(session_pageviews) AS t(pv)
    """
    df = db.query(qry).df() 
    db.close()
    df['pageview_id'] = [uuid.uuid4().hex for _ in range(df.shape[0])]
    return df

def gen_pond_water(swamp_water):
    session_cols = ['session_marketing_channel','session_date','user_id','session_id','session_sequence_number','session_start_time','session_browser','session_os','session_is_mobile','session_device_category','session_country','session_city','session_region','session_source','session_medium','session_revenue','session_total_revenue','session_order_cnt','session_pageview_cnt','session_duration','session_is_first_visit','session_landing_screen','session_exit_screen']
    sessions_df = swamp_water[session_cols]
    event_cols = ['user_id','session_id','session_start_time','session_date','session_events'] 
    pageviews_df = swamp_water[event_cols]
    pageviews_df['session_events'] = pageviews_df['session_events'].apply(filter_keys)
    pageviews_df['session_pageviews'] = pageviews_df.apply(lambda row: construct_pageview_event_list(row['session_events'], row['session_date']), axis=1)
    mask = pageviews_df['session_pageviews'].apply(len) > 1
    pageviews_df.loc[mask, 'session_pageviews'] = pageviews_df.loc[mask, 'session_pageviews'].apply(adjust_timestamps)
    pageviews_df.drop(columns=['session_events'], inplace=True)
    pageviews_df = explode_pageviews(pageviews_df)
    return sessions_df, pageviews_df 

for swamp_water_bucket in glob.glob("./data_swamp/*.csv"):
    file_date = re.search(r'\d+\-\d+\-\d+', swamp_water_bucket).group(0)
    print('Processing swamp water from: ' + file_date)
    swamp_water = pd.read_csv(swamp_water_bucket)
    print('Filtering swamp water...')
    filtered_swamp_water = filter_swamp_water(swamp_water)
    print('Generating pond water...')
    sessions_df, pageviews_df = gen_pond_water(filtered_swamp_water)
    sessions_df.to_csv('./data_pond/sessions/sessions_' + file_date + '.csv', index=False)
    pageviews_df.to_csv('./data_pond/pageviews/pageviews_' + file_date + '.csv', index=False)