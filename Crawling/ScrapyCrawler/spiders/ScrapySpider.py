# 필요한 모듈 불러오기
import requests
from bs4 import BeautifulSoup
import scrapy
from ..Items import MPBMinuteCrawlingItem, BondReportCrawlingItem

# ----------------------------------------------------------------------------------------------------

# BondReportSpider() 클래스 정의
class MPBMinuteSpider(scrapy.Spider):

    # 스파이더 이름 정의
    name = "MPBMinuteCrawler"

    # 쿼리 스트링(Query String)을 제외한 기본 URL을 base_url에 할당
    base_url = "https://www.bok.or.kr/portal/bbs/B0000245/list.do"

    # ----------------------------------------------------------------------------------------------------

    # start_requests() 함수 정의
    def start_requests(self):
        """
        크롤링을 수행할 웹 페이지에 HTTPS 요청을 보내고 콜백 함수로서 hwp_locator() 함수를 호출하는 함수입니다.\n
        scrapy 모듈의 Request 클래스 객체를 반환합니다.
        """

        # 쿼리 스트링(Query String)을 query_url에 할당
        query_url = "?menuNo=200761&pageIndex=1"

        # GET 요청을 보내고 응답 코드를 출력
        res = requests.get(self.base_url + query_url)
        print(">>> 한국은행 금융통화위원회 의사록 웹 페이지의 응답 코드: {0}\n".format(res.status_code))

        # 해당 웹 페이지의 HTML 코드 텍스트를 가져와 변수 soup에 할당
        soup = BeautifulSoup(res.text, "html.parser")

        # HTML 코드에서 웹 페이지 주소 쿼리 스트링(Query String)에 사용할 pageNm 추출
        pageNm = int(soup.select_one(".i.end a").attrs["href"].split("=")[-1])

        #content > div.listBottom > div > ul > li.i.end > a
        # for 반복문을 통해 각 페이지를 순회
        for page in range(1, pageNm + 1, 1):

            # 쿼리 스트링(Query String)을 query_page_url에 할당
            query_page_url = "?menuNo=200761&pageIndex="+ str(page)
        
            # 결과 값 반환
            yield scrapy.Request(url = self.base_url + query_page_url, callback = self.hwp_locator)

    # ----------------------------------------------------------------------------------------------------

    # hwp_locator() 함수 정의
    def hwp_locator(self, response):
        """
        웹 페이지에서 HWP 파일의 주소를 추출하여 반환하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """

        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # MPBMinuteCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = MPBMinuteCrawlingItem()

        # for 반복문을 사용해 각 리포트를 순회
        for list_num in range(1, 11):
            
            try:
                # 의사록 제목을 추출해 "title" 열에 저장
                item["title"] = self.title_preprocessor(response.xpath("//*[@id='content']/div[3]/ul/li[{0}]/div/span/a/span/span/text()".format(list_num)).extract())

                # 의사록 HWP 파일 주소를 추출해 "file_url" 열에 저장 
                try:
                    item["file_url"] = ["https://www.bok.or.kr" + response.xpath("//*[@id='content']/div[3]/ul/li[{0}]/div/div[1]/div/div/ul/li[1]/a[1]/@href".format(list_num)).extract()[0]]
                except:
                    item["file_url"] = ["https://www.bok.or.kr" + response.xpath("//*[@id='content']/div[3]/ul/li[{0}]/div/div[1]/a[1]/@href".format(list_num)).extract()[0]]

                # 의사록 작성일자를 추출해 "date" 열에 저장
                item["date"] = self.date_preprocessor(response.xpath("//*[@id='content']/div[3]/ul/li[{0}]/div/span/a/span/span/text()".format(list_num)).extract())

                # 결과 값 반환
                yield item

            # 오류 발생 시 for 반복문으로 회귀
            except:
                continue

    # ----------------------------------------------------------------------------------------------------

    # date_preprocessor() 함수 정의
    def title_preprocessor(self, crawled_title : list) -> list:
        """
        한국은행 웹 페이지에서 크롤링해 온 의사록 제목에서 날짜를 제거하고 정리하는 함수입니다.\n
        의사록 제목으로 구성된 리스트를 반환합니다.
        """

        # 결과 값이 존재하지 않는 경우 제목에 대한 전처리를 수행하지 않도록 결과 값을 그대로 반환
        if not crawled_title:
            return crawled_title

        # 크롤링해 온 제목에서 괄호를 제거하고 띄어쓰기를 "_"로 대체
        title = crawled_title[0].split("(")[1].replace(")", "").replace(" ", "_")

        # 결과 값 반환
        return [f"{title}_금융통화위원회_의사록"]

    # ----------------------------------------------------------------------------------------------------

    # date_preprocessor() 함수 정의
    def date_preprocessor(self, crawled_date : list) -> list:
        """
        한국은행 웹 페이지에서 크롤링해 온 날짜를 4자리의 연도, 2자리의 월, 2자리의 일자로 변환하는 함수입니다.\n
        날짜로 구성된 리스트를 반환합니다.
        """

        # 크롤링해 온 날짜에서 괄호를 제거하고 연도, 월, 일 분리
        date = crawled_date[0].split("(")[-1].replace(")", "")
        try:
            year, month, day = date.split(".")
        except:
            year, month, day, pad = date.split(".")

        # zfill() 함수를 사용해 월과 일을 두 자리의 숫자가 되도록 0을 채우기
        month = month.zfill(2)
        day = day.zfill(2)

        # 결과 값 반환
        return [f"{year}.{month}.{day}"]

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
        크롤링을 수행할 웹 페이지에 HTTPS 요청을 보내고 콜백 함수로서 pdf_locator() 함수를 호출하는 함수입니다.\n
        scrapy 모듈의 Request 클래스 객체를 반환합니다.
        """

        # 쿼리 스트링(Query String)을 query_url에 할당
        query_url = "?&page=1"

        # GET 요청을 보내고 응답 코드를 출력
        res = requests.get(self.base_url + query_url)
        print(">>> 네이버 채권분석 리포트 웹 페이지의 응답 코드: {0}\n".format(res.status_code))

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
        웹 페이지에서 PDF 파일의 주소를 추출하여 반환하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """

        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # BondReportCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = BondReportCrawlingItem()

        # for 반복문을 사용해 각 리포트를 순회
        for list_num in range(2, 48):

            # 구분선인 경우 저장하지 않고 제외
            if not response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/text()".format(list_num)).extract():
                continue

            else:
                # 리포트 제목, 증권사, PDF 파일 주소, 작성일자를 추출해 각 열에 저장
                item["title"] = response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[1]/a/text()".format(list_num)).extract()
                item["writer"] = response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[2]/text()".format(list_num)).extract()
                item["file_url"] = response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[3]/a/@href".format(list_num)).extract()
                item["date"] = self.date_preprocessor(response.xpath("//*[@id='contentarea_left']/div[3]/table[1]/tr[{0}]/td[4]/text()".format(list_num)).extract())

                # 결과 값 반환
                yield item
    
    # ----------------------------------------------------------------------------------------------------

    # date_preprocessor() 함수 정의
    def date_preprocessor(self, crawled_date : list) -> list:
        """
        네이버 채권 리포트 웹 페이지에서 크롤링해 온 날짜를 4자리의 연도, 2자리의 월, 2자리의 일자로 변환하는 함수입니다.\n
        날짜로 구성된 리스트를 반환합니다.
        """

        # 크롤링해 온 날짜에서 연도를 추가
        date = "20" + crawled_date[0]

        # 결과 값 반환
        return [date]