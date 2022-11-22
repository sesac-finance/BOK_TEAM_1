# 필요한 모듈 불러오기
import requests
from bs4 import BeautifulSoup
import scrapy
from ..Items import NewsCrawlingItem

# ----------------------------------------------------------------------------------------------------

# NewsURLSpider() 클래스 정의
class NewsURLSpider(scrapy.Spider):

    # 스파이더 이름 정의
    name = "NewsURLCrawler"

    # 쿼리 스트링(Query String)을 제외한 기본 URL을 base_url에 할당
    base_url = "https://search.naver.com/search.naver"

    # ----------------------------------------------------------------------------------------------------

    # start_requests() 함수 정의
    def start_requests(self):
        """
        크롤링을 수행할 웹 페이지에 HTTPS 요청을 보내고 콜백 함수로서 hankyung_news_parse() 함수를 호출하는 함수입니다.\n
        scrapy 모듈의 Request 클래스 객체를 반환합니다.
        """

        # 3개월 단위로 검색할 검색 시작 일자와 검색 종료 일자를 설정
        start_date = [
            "20121101", "20130201", "20130501", "20130801", "20131101", "20140201", "20140501", "20140801",
            "20141101", "20150201", "20150501", "20150801", "20151101", "20160201", "20160501", "20160801",
            "20161101", "20170201", "20170501", "20170801", "20171101", "20180201", "20180501", "20180801",
            "20181101", "20190201", "20190501", "20190801", "20191101", "20200201", "20200501", "20200801",
            "20201101", "20210201", "20210501", "20210801", "20211101", "20220201", "20220501", "20220801"
        ]
        end_date = [
            "20130131", "20130430", "20130731", "20131030", "20140131", "20140430", "20140731", "20141030",
            "20150131", "20150430", "20150731", "20151030", "20160131", "20160430", "20160731", "20161030",
            "20170131", "20170430", "20170731", "20171030", "20180131", "20180430", "20180731", "20181030",
            "20190131", "20190430", "20190731", "20191030", "20200131", "20200430", "20200731", "20201030",
            "20210131", "20210430", "20210731", "20211030", "20220131", "20220430", "20220731", "20221030"
        ]

        for idx in range(40):

        # 쿼리 스트링(Query String)을 query_url에 할당
            query_url = '?where=news&query="기준금리"&sort=1&news_office_checked=1015&nso=p:from{0}to{1}&start={2}1'.format(start_date[idx], end_date[idx], page)

        # news_office_checked=1011 : 서울경제
        # 1015 : 한국경제
        # 1018: 이데일리


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