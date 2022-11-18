# 필요한 모듈 불러오기
import requests
from bs4 import BeautifulSoup
import scrapy
from ScrapyCrawler.Items import BondReportCrawlingItem

# ----------------------------------------------------------------------------------------------------

# BondReportSpider() 클래스 정의
class BondReportSpider(scrapy.Spider):

    # 스파이더 이름 정의
    name = "BondReportCrawler"

    # 쿼리 스트링(Query String)을 제외한 기본 URL을 base_url에 할당
    base_url = "https://finance.naver.com/research/debenture_list.naver"

    # ----------------------------------------------------------------------------------------------------

    # start_requests() 함수 정의
    def start_requests(self):
        """
        크롤링을 수행할 웹 페이지에 HTTPS 요청을 보내고 콜백 함수로서 incruit_parse() 함수를 호출하는 함수입니다.\n
        scrapy 모듈의 Request 클래스 객체를 반환합니다.
        """

        # 쿼리 스트링(Query String)을 query_url에 할당
        query_url = "?&page=1"

        # GET 요청을 보내고 응답 코드를 출력
        res = requests.get(self.base_url + query_url)
        print(">>> 네이버 채권분석 리포트 웹 페이지의 응답 코드: {0}".format(res.status_code))

        # 해당 웹 페이지의 HTML 코드 텍스트를 가져와 변수 soup에 할당
        soup = BeautifulSoup(res.text, "html.parser")

        # HTML 코드에서 웹 페이지 주소 쿼리 스트링(Query String)에 사용할 pageNm 추출
        pageNm = int(soup.select_one("table.Nnavi td.pgRR a").attrs["href"].split("=")[1])

        # for 반복문을 통해 각 페이지를 순회
        for page in range(1, pageNm + 1, 1):

            # 쿼리 스트링(Query String)을 query_page_url에 할당
            query_page_url = "?&page="+ str(page)
        
            # 결과 값 반환
            yield scrapy.Request(url = self.base_url + query_page_url, callback = self.pdf_locator)

    # ----------------------------------------------------------------------------------------------------

    # pdf_locator() 함수 정의
    def pdf_locator(self, response):
        """
        웹 페이지에서 PDF 파일의 주소를 추출하여 반환하는 함수입니다.
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """

        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}")

        # BondReportCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = BondReportCrawlingItem()

        # for 반복문을 사용해 각 리포트를 순회
        for list_num in range(2, 48):

            # 구분선인 경우 저장하지 않고 제외
            if not response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/text()".format(list_num)).extract():
                continue

            else:
                # 리포트 제목, 증권사, PDF 파일 주소, 작성일자을 추출해 각 열에 저장
                item["title"] = response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[1]/a/text()".format(list_num)).extract()
                item["writer"] = response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[2]/text()".format(list_num)).extract()
                item["file_url"] = response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[3]/a/@href".format(list_num)).extract()
                item["date"] = response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[4]/text()".format(list_num)).extract()

                # 결과 값 반환
                yield item