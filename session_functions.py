# Auto-generated session-related GA4 functions

from google.cloud import bigquery
bigquery_client = bigquery.Client()

def get_marketing_channel_drove_highest_number_sessions_flat_ses(start_date, end_date):
    """
    Function get_marketing_channel_drove_highest_number_sessions_flat_ses(start_date, end_date):
    Returns the marketing channel (traffic_source) with the highest session count.
    """
    sql = f"""
        SELECT traffic_source, COUNT(*) AS sessions
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY traffic_source
        ORDER BY sessions DESC
        LIMIT 1
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_compare_number_sessions_desktop_vs_mobile_flat_sessions(start_date, end_date):
    """
    Function get_compare_number_sessions_desktop_vs_mobile_flat_sessions(start_date, end_date):
    Compares number of sessions between desktop and mobile device categories.
    """
    sql = f"""
        SELECT device_category, COUNT(*) AS sessions
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
          AND device_category IN ('desktop', 'mobile')
        GROUP BY device_category
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_average_number_pageviews_per_session_flat_sessions(start_date, end_date):
    """
    Function get_average_number_pageviews_per_session_flat_sessions(start_date, end_date):
    Returns the average number of pageviews per session.
    """
    sql = f"""
        SELECT ROUND(AVG(pageviews), 2) AS avg_pageviews
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_identify_sessions_only_one_pageview_bounces_flat_session(start_date, end_date):
    """
    Function get_identify_sessions_only_one_pageview_bounces_flat_session(start_date, end_date):
    Returns all sessions where only one pageview occurred (bounces).
    """
    sql = f"""
        SELECT session_id, user_pseudo_id, session_start_date
        FROM `your_project.dataset.flat_sessions`
        WHERE pageviews = 1
          AND session_start_date BETWEEN '{start_date}' AND '{end_date}'
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_many_users_visited_specific_campaign_flat_sessions(campaign_name, start_date, end_date):
    """
    Function get_many_users_visited_specific_campaign_flat_sessions(campaign_name, start_date, end_date):
    Returns number of unique users who visited from a specific campaign.
    """
    sql = f"""
        SELECT COUNT(DISTINCT user_pseudo_id) AS users
        FROM `your_project.dataset.flat_sessions`
        WHERE campaign_name = '{campaign_name}'
          AND session_start_date BETWEEN '{start_date}' AND '{end_date}'
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_referral_source_drove_most_engaged_traffic_flat_sessions(start_date, end_date):
    """
    Function get_referral_source_drove_most_engaged_traffic_flat_sessions(start_date, end_date):
    Returns the referral source with the highest average pageviews per session.
    """
    sql = f"""
        SELECT traffic_source, ROUND(AVG(pageviews), 2) AS avg_pageviews
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY traffic_source
        ORDER BY avg_pageviews DESC
        LIMIT 1
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_sessions_browser_os_combinations_flat_sessions(start_date, end_date):
    """
    Function get_sessions_browser_os_combinations_flat_sessions(start_date, end_date):
    Returns session counts grouped by browser and OS (approximated using platform and device_category).
    """
    sql = f"""
        SELECT platform, device_category, COUNT(*) AS sessions
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY platform, device_category
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_did_session_volume_change_weekoverweek_flat_sessions(start_date, end_date):
    """
    Function get_did_session_volume_change_weekoverweek_flat_sessions(start_date, end_date):
    Compares weekly session volumes in the given date range.
    """
    sql = f"""
        SELECT
            FORMAT_DATE('%Y-%W', DATE(session_start_date)) AS week,
            COUNT(*) AS sessions
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY week
        ORDER BY week
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_landing_pages_are_most_common_new_flat_sessions(start_date, end_date):
    """
    Function get_landing_pages_are_most_common_new_flat_sessions(start_date, end_date):
    Returns most common landing pages for new users (proxied by first session date).
    """
    sql = f"""
        SELECT campaign_name AS landing_page, COUNT(*) AS new_user_sessions
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY landing_page
        ORDER BY new_user_sessions DESC
        LIMIT 10
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_sessions_organic_traffic_flat_sessions(start_date, end_date):
    """
    Function get_sessions_organic_traffic_flat_sessions(start_date, end_date):
    Returns the number of sessions from organic traffic between the given dates.
    """
    sql = f"""
        SELECT COUNT(*) AS sessions
        FROM `your_project.dataset.flat_sessions`
        WHERE traffic_source = 'organic'
          AND session_start_date BETWEEN '{start_date}' AND '{end_date}'
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_average_session_duration_device_category_flat_sessions(start_date, end_date):
    """
    Function get_average_session_duration_device_category_flat_sessions(start_date, end_date):
    Returns average session duration (in seconds) grouped by device category.
    """
    sql = f"""
        SELECT device_category,
               ROUND(AVG(session_duration), 2) AS avg_duration_seconds
        FROM (
            SELECT device_category,
                   (LEAD(session_start_ts) OVER (PARTITION BY user_pseudo_id ORDER BY session_start_ts)
                    - session_start_ts) / 1000000 AS session_duration
            FROM `your_project.dataset.flat_sessions`
            WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        )
        WHERE session_duration IS NOT NULL
        GROUP BY device_category
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_country_had_highest_number_sessions_flat_sessions(start_date, end_date):
    """
    Function get_country_had_highest_number_sessions_flat_sessions(start_date, end_date):
    Returns the country with the highest number of sessions in the given period.
    """
    sql = f"""
        SELECT country, COUNT(*) AS sessions
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY country
        ORDER BY sessions DESC
        LIMIT 1
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_bounce_rate_traffic_source_flat_sessions(start_date, end_date):
    """
    Function get_bounce_rate_traffic_source_flat_sessions(start_date, end_date):
    Returns the bounce rate per traffic source.
    """
    sql = f"""
        SELECT
            traffic_source,
            ROUND(SUM(IF(pageviews = 1, 1, 0)) / COUNT(*) * 100, 2) AS bounce_rate_percentage
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY traffic_source
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_percentage_sessions_resulted_purchase_flat_sessions(start_date, end_date):
    """
    Function get_percentage_sessions_resulted_purchase_flat_sessions(start_date, end_date):
    Returns the percentage of sessions that resulted in at least one purchase.
    """
    sql = f"""
        SELECT
            ROUND(SUM(IF(purchases > 0, 1, 0)) / COUNT(*) * 100, 2) AS purchase_conversion_rate
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
    """
    return [dict(row) for row in bigquery_client.query(sql)]

def get_conversion_rate_traffic_source_flat_sessions(start_date, end_date):
    """
    Function get_conversion_rate_traffic_source_flat_sessions(start_date, end_date):
    Returns the conversion rate (purchase sessions / total) grouped by traffic source.
    """
    sql = f"""
        SELECT
            traffic_source,
            ROUND(SUM(IF(purchases > 0, 1, 0)) / COUNT(*) * 100, 2) AS conversion_rate
        FROM `your_project.dataset.flat_sessions`
        WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY traffic_source
    """
    return [dict(row) for row in bigquery_client.query(sql)]

