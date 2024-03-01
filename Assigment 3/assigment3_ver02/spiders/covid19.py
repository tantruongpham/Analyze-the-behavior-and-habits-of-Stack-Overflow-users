import scrapy
from scrapy_splash import SplashRequest
import re

class Covid19Spider(scrapy.Spider):
    name = "covid19"
    allowed_domains = ['web.archive.org']
    start_urls = ["https://web.archive.org/web/20210519041814/https://ncov.moh.gov.vn/vi/web/guest/dong-thoi-gian?p_p_id=com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_nf7Qy5mlPXqs&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_nf7Qy5mlPXqs_delta=10&p_r_p_resetCur=false&_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_nf7Qy5mlPXqs_cur=23"]

    # Lua Script của Splash
    script = '''
    function main(splash, args)
        url = args.url
        assert(splash:go(url))
        assert(splash:wait(1))
        splash:set_viewport_full()
        return {
            html = splash:html(),
            png = splash:png(),
        }
    end
    '''
    
    def start_requests(self):
        for url in self.start_urls: 
            yield SplashRequest(url, callback=self.parse, endpoint="execute", args={
                'lua_source': self.script, 'timeout': 90
            })
    
    @staticmethod
    def extract_case_number(input_string):
        """Hàm để lấy số case từ chuỗi đầu vào"""
        regex_pattern = r"\b(\d+\.\d+|\d+)\b"  # Biểu thức chính quy để tìm các con số trong chuỗi
        matches = re.findall(regex_pattern, input_string)
        if matches:
            return int(matches[0].replace('.', ''))
        else:
            return None

    def parse(self, response):
        for info in response.xpath("//div[contains(@class,'timeline-detail')]"):
            time = info.xpath(".//div[1]/h3/text()").get()
            newcase = info.xpath(".//div[2]/p[2]/text()").get()
            newcase = self.extract_case_number(newcase)

            yield {
                'time': time,
                'newcase': newcase
            }
      
        # Lấy URL trang tiếp theo
        next_url = response.css("ul.lfr-pagination-buttons.pager li a:contains('Tiếp theo')::attr(href)").get()
        
        if next_url:
            # Tạo SplashRequest cho trang tiếp theo
            yield SplashRequest(
                url=next_url,
                callback=self.parse,
                meta={
                    "splash": {"endpoint": "execute", "args": {"lua_source": self.script}}
                }
            )
