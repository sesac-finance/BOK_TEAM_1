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