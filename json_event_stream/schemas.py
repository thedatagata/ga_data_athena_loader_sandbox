from pydantic import BaseModel, constr
from typing import Optional

class PageviewsSchema(BaseModel):
    user_id: str
    session_id: str
    session_start_time: int  
    pageview_timestamp: str
    hostname: str
    page_path: str
    page_title: str
    page_path_level1: str
    page_path_level2: Optional[str] = None
    page_path_level3: Optional[str] = None
    total_product_impressions: Optional[int] = None 
    pageview_id: str
    pageview_month: str
    page_path_level4: Optional[str] = None

class SessionsSchema(BaseModel):
    session_marketing_channel: str
    session_date: str
    user_id: str
    session_id: str
    session_sequence_number: int 
    session_start_time: int
    session_browser: str
    session_os: str
    session_is_mobile: bool
    session_device_category: str
    session_country: str
    session_source: str
    session_medium: str
    session_revenue: Optional[float] = None 
    session_total_revenue: Optional[float] = None
    session_order_cnt: Optional[int] = None 
    session_pageview_cnt: Optional[int] = None
    session_duration: Optional[int] = None
    session_is_first_visit: bool
    session_landing_screen: str
    session_exit_screen: str
    session_month: str
    session_city: str
    session_region: str