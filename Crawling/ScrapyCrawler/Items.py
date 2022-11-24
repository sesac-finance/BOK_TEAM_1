# scrapy 모듈 불러오기
import scrapy

# ----------------------------------------------------------------------------------------------------

# MPBMinuteCrawlingItem() 클래스 정의
class MPBMinuteCrawlingItem(scrapy.Item):

    # 크롤링을 통해 저장할 의사록 제목, HWP 파일 주소, 작성일자를 속성으로 정의
    title = scrapy.Field()
    file_url = scrapy.Field()
    date = scrapy.Field()

# ----------------------------------------------------------------------------------------------------

# BondReportCrawlingItem() 클래스 정의
class BondReportCrawlingItem(scrapy.Item):

    # 크롤링을 통해 저장할 리포트 제목, 증권사, PDF 파일 주소, 작성일자를 속성으로 정의
    title = scrapy.Field()
    writer = scrapy.Field()
    file_url = scrapy.Field()
    date = scrapy.Field()

# ----------------------------------------------------------------------------------------------------

# NewsURLCrawlingItem() 클래스 정의
class NewsURLCrawlingItem(scrapy.Item):

    # 크롤링을 통해 저장할 뉴스사, 뉴스 웹 페이지 주소, 작성일자를 속성으로 정의
    news_office = scrapy.Field()
    news_url = scrapy.Field()
    date = scrapy.Field()

# ----------------------------------------------------------------------------------------------------

# NewsContentCrawlingItem() 클래스 정의
class NewsContentCrawlingItem(scrapy.Item):

    # 크롤링을 통해 저장할 뉴스사, 뉴스 제목, 뉴스 본문, 작성일자를 속성으로 정의
    news_office = scrapy.Field()
    news_title = scrapy.Field()
    news_body = scrapy.Field()
    date = scrapy.Field()