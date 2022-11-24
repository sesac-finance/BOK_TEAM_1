# 필요한 모듈 불러오기
import pandas as pd
import scrapy
from ..Items import NewsURLCrawlingItem, NewsContentCrawlingItem
import re
import string

# ----------------------------------------------------------------------------------------------------

# NewsURLSpider() 클래스 정의
class NewsURLSpider(scrapy.Spider):

    # 스파이더 이름 정의
    name = "NewsURLCrawler"

    # 쿼리 스트링(Query String)을 제외한 기본 URL을 base_url에 할당
    base_url = "https://search.naver.com/search.naver"

    # 신문사 쿼리 스트링을 담은 딕셔너리 news_office 초기화
    news_offices = {1008: "머니투데이", 1009: "매일경제", 1011: "서울경제", 1015: "한국경제", 1016: "헤럴드경제"}

    # ----------------------------------------------------------------------------------------------------

    # start_requests() 함수 정의
    def start_requests(self):
        """
        크롤링을 수행할 웹 페이지에 HTTPS 요청을 보내고 콜백 함수로서 news_locator() 함수를 호출하는 함수입니다.\n
        scrapy 모듈의 Request 클래스 객체를 반환합니다.
        """

        # 3개월 단위로 검색할 검색 시작일자와 검색 종료일자를 설정
        start_date = [day.strftime("%Y%m%d") for day in (pd.date_range(start = "20130101", end = "20221231", freq = "QS") + pd.DateOffset(months = -2))]
        end_date = [day.strftime("%Y%m%d") for day in (pd.date_range(start = "20130101", end = "20221231", freq = "Q") + pd.DateOffset(months = -2))]
        end_date = [day.replace("0730", "0731") for day in end_date]

        # for 반복문을 사용해 각 신문사를 순회
        for news_office in self.news_offices.keys():

            # for 반복문을 사용해 각 기간을 순회
            for idx in range(40):

                # 크롤링할 페이지를 나타내는 변수 page 초기화
                page = 0

                # 쿼리 스트링(Query String)을 query_url에 할당
                query_url = '?where=news&query="금리"&sort=1&mynews=1&news_office_checked={0}&nso=p:from{1}to{2}&start={3}1'.format(
                    news_office,
                    start_date[idx],
                    end_date[idx],
                    page
                )

                # 결과 값 반환
                yield scrapy.Request(url = self.base_url + query_url, callback = self.news_locator, cb_kwargs = dict(
                    news_office = news_office,
                    start_date = start_date,
                    end_date = end_date,
                    idx = idx,
                    page = page
                ))

    # ----------------------------------------------------------------------------------------------------

    # news_locator() 함수 정의
    def news_locator(self, response, news_office : int, start_date : list, end_date : list, idx : int, page : int):
        """
        웹 페이지에서 신문사, 뉴스기사 URL, 뉴스기사 날짜를 추출하고 다음 페이지를 호출하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체와 Request 클래스 객체를 반환합니다.
        """

        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # 검색결과가 없는 경우 안내 메시지 출력 후 더 이상 크롤링을 수행하지 않도록 설정
        if response.css(".not_found02").get():
            print(">>> 검색 결과가 존재하지 않습니다.")
            if idx == 39: pass
            else: print(">>> 검색 결과가 존재하지 않아 검색 기간을 재설정합니다: {0} - {1}\n".format(start_date[idx + 1], end_date[idx + 1]))
            yield None

        else:
        
            # NewsURLCrawlingItem() 클래스를 가져와 변수 item에 할당
            item = NewsURLCrawlingItem()

            # for 반복문을 사용해 각 뉴스 기사에 접근
            for list_num in range(1, 11):

                # 뉴스 기사의 ID에 접근하기 위해 계산
                news_num = list_num + page * 10

                # 신문사, 뉴스기사 URL, 뉴스기사 날짜를 추출해 각 열에 저장
                item["news_office"] = self.news_offices[news_office]
                item["news_url"] = response.xpath("//*[@id='sp_nws{0}']/div/div/a/@href".format(news_num)).extract()
                item["date"] = response.xpath("//*[@id='sp_nws{0}']/div/div/div[1]/div[2]/span/text()".format(news_num)).extract()[-1 :]
            
                # 결과 값 반환
                yield item

            # 다음 페이지로 페이지 수 조정 후 다음 페이지 호출
            page += 1

            # 쿼리 스트링(Query String)을 query_url에 할당
            query_url = '?where=news&query="금리"&sort=1&mynews=1&news_office_checked={0}&nso=p:from{1}to{2}&start={3}1'.format(
                news_office,
                start_date[idx],
                end_date[idx],
                page
            )

            # 결과 값 반환
            yield scrapy.Request(url = self.base_url + query_url, callback = self.news_locator, cb_kwargs = dict(
                news_office = news_office,
                start_date = start_date,
                end_date = end_date,
                idx = idx,
                page = page
            ))

# ----------------------------------------------------------------------------------------------------

# NewsContentSpider() 클래스 정의
class NewsContentSpider(scrapy.Spider):

    # 스파이더 이름 정의
    name = "NewsContentCrawler"

    # ----------------------------------------------------------------------------------------------------

    # start_requests() 함수 정의
    def start_requests(self):
        """
        크롤링을 수행할 웹 페이지에 HTTPS 요청을 보내고 신문사에 따라 각각의 콜백 함수를 호출하는 함수입니다.\n
        scrapy 모듈의 Request 클래스 객체를 반환합니다.
        """

        # 크롤링을 위해 뉴스 URL을 담은 CSV 파일을 불러와 데이터프레임 생성
        news_df = pd.read_csv("./../Data/Location/News_Location.csv", header = 0, encoding = "utf-8-sig")

        # for 반복문을 사용하여 데이터프레임의 각 행을 순회
        for idx in news_df.index:

            # 뉴스 기사의 URL, 신문사, 기사 작성일자를 추출
            news_office = news_df.loc[idx, "news_office"]
            news_url = news_df.loc[idx, "news_url"]
            date = news_df.loc[idx, "date"]

            # 머니투데이일 경우 money_parser() 함수 호출
            if news_office == "머니투데이":
                yield scrapy.Request(url = news_url, callback = self.money_parser, cb_kwargs = dict(news_office = news_office, date = date))

            # 매일경제일 경우 maeil_parser() 함수 호출
            elif news_office == "매일경제":
                yield scrapy.Request(url = news_url, callback = self.maeil_parser, cb_kwargs = dict(news_office = news_office, date = date))

            # 서울경제일 경우 seoul_parser() 함수 호출
            elif news_office == "서울경제":
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
                yield scrapy.Request(url = news_url, callback = self.seoul_parser, headers = headers, cb_kwargs = dict(news_office = news_office, date = date))

            # 한국경제일 경우 korea_parser() 함수 호출
            elif news_office == "한국경제":
                yield scrapy.Request(url = news_url, callback = self.korea_parser, cb_kwargs = dict(news_office = news_office, date = date))

            # 헤럴드경제일 경우 herald_parser() 함수 호출
            else:
                yield scrapy.Request(url = news_url, callback = self.herald_parser, cb_kwargs = dict(news_office = news_office, date = date))

    # ----------------------------------------------------------------------------------------------------

    # money_parser() 함수 정의
    def money_parser(self, response, news_office : str, date : str):
        """
        머니투데이 웹 페이지에서 뉴스기사 제목과 뉴스기사 내용을 추출하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """
        
        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # NewsURLCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = NewsContentCrawlingItem()

        # 뉴스기사 내용을 추출해 변수 news_body에 할당
        news_body = " ".join([text.strip() for text in response.xpath("//*[@id='textBody']/text()").extract()]).strip()

        # 신문사, 뉴스기사 제목, 뉴스기사 내용, 뉴스기사 날짜를 추출해 각 열에 저장
        item["news_office"] = news_office
        item["news_title"] = response.css(".view_top h1::text").get().strip()
        item["news_body"] = self.news_body_cleanser(news_body)
        item["date"] = date

        # 결과 값 반환
        yield item

    # ----------------------------------------------------------------------------------------------------

    # maeil_parser() 함수 정의
    def maeil_parser(self, response, news_office : str, date : str):
        """
        매일경제 웹 페이지에서 뉴스기사 제목과 뉴스기사 내용을 추출하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """
    
        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # NewsURLCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = NewsContentCrawlingItem()

        # 매일경제 웹 페이지 크롤링 코드를 시도
        try:

            # 뉴스기사 내용을 추출해 변수 news_body에 할당
            news_body = " ".join([text.strip() for text in response.xpath("//*[@id='container']/section/div[3]/section/div[1]/div[1]/div[1]/text()").extract()]).strip()

            # 신문사, 뉴스기사 제목, 뉴스기사 내용, 뉴스기사 날짜을 각 열에 저장
            item["news_office"] = news_office
            item["news_title"] = response.css("div.news_ttl_sec h2::text").get().strip()
            item["news_body"] = self.news_body_cleanser(news_body)
            item["date"] = date
        
            # 결과 값 반환
            yield item

        # 오류 발생 시 매경프리미엄 웹 페이지 크롤링 코드를 시도
        except:

            # 뉴스기사 내용을 추출해 변수 news_body에 할당
            news_body = " ".join([text.strip() for text in response.css(".view_txt::text").extract()]).strip()

            # 신문사, 뉴스기사 제목, 뉴스기사 내용, 뉴스기사 날짜을 각 열에 저장
            item["news_office"] = news_office
            item["news_title"] = response.css(".view_title h1::text").get().strip()
            item["news_body"] = self.news_body_cleanser(news_body)
            item["date"] = date
        
            # 결과 값 반환
            yield item

    # ----------------------------------------------------------------------------------------------------

    # seoul_parser() 함수 정의
    def seoul_parser(self, response, news_office : str, date : str):
        """
        서울경제 웹 페이지에서 뉴스기사 제목과 뉴스기사 내용을 추출하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """
        
        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # NewsURLCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = NewsContentCrawlingItem()

        # 네이버 뉴스 홈페이지인 경우 신문사, 뉴스기사 제목, 뉴스기사 내용, 뉴스기사 날짜를 추출해 각 열에 저장
        if "https://n.news.naver.com" in url:
            news_body = " ".join([text.strip() for text in response.xpath("//*[@id='dic_area']/text()").extract()]).strip()
            item["news_office"] = news_office
            item["news_title"] =  response.xpath("//*[@id='ct']/div[1]/div[2]/h2/text()").get()
            item["news_body"] = self.news_body_cleanser(news_body)
            item["date"] = date
 
            # 결과 값 반환
            yield item

        # 서울경제 홈페이지인 경우 신문사, 뉴스기사 제목, 뉴스기사 내용, 뉴스기사 날짜를 추출해 각 열에 저장
        else:
            news_body = " ".join([text.strip() for text in response.css(".article_view::text").extract()]).strip()
            item["news_office"] = news_office
            item["news_title"] =  response.css(".article_head h1::text").get().strip()
            item["news_body"] = self.news_body_cleanser(news_body)
            item["date"] = date

            # 결과 값 반환
            yield item

    # ----------------------------------------------------------------------------------------------------

    # korea_parser() 함수 정의
    def korea_parser(self, response, news_office : str, date : str):
        """
        한국경제 웹 페이지에서 뉴스기사 제목과 뉴스기사 내용을 추출하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """
        
        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # NewsURLCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = NewsContentCrawlingItem()

        # 뉴스기사 내용을 추출해 변수 news_body에 할당
        news_body = " ".join([text.strip() for text in response.css(".article-body::text").extract()]).strip()

        # 신문사, 뉴스기사 제목, 뉴스기사 내용, 뉴스기사 날짜를 추출해 각 열에 저장
        item["news_office"] = news_office
        try:
            item["news_title"] =  response.css(".headline::text").get().strip()
        except:
            item["news_title"] =  response.css(".article-tit::text").get().strip()
        item["news_body"] = self.news_body_cleanser(news_body)
        item["date"] = date
   
        # 결과 값 반환
        yield item

    # ----------------------------------------------------------------------------------------------------

    # herald_parser() 함수 정의
    def herald_parser(self, response, news_office : str, date : str):
        """
        헤럴드경제 웹 페이지에서 뉴스기사 제목과 뉴스기사 내용을 추출하는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """
        
        # 현재 크롤링을 진행하고 있는 웹 페이지 주소를 출력
        url = response.url
        print(f">>> 다음 URL을 크롤링 중입니다: {url}\n")

        # NewsURLCrawlingItem() 클래스를 가져와 변수 item에 할당
        item = NewsContentCrawlingItem()

        # 뉴스기사 내용을 추출해 변수 news_body에 할당
        news_body = " ".join([text.strip() for text in response.css("#articleText ::text").extract()]).strip()

        # 신문사, 뉴스기사 제목, 뉴스기사 내용, 뉴스기사 날짜를 추출해 각 열에 저장
        item["news_office"] = news_office
        item["news_title"] =  response.css(".article_title.ellipsis2::text").get().strip()
        item["news_body"] = self.news_body_cleanser(news_body)
        item["date"] = date
   
        # 결과 값 반환
        yield item

    # ----------------------------------------------------------------------------------------------------

    # news_body_cleanser() 함수 정의
    def news_body_cleanser(self, news_body : str) -> str :
        """
        뉴스기사 내용을 가져와 문장 부호와 불필요한 내용을 제거하는 함수입니다.\n
        전처리된 뉴스기사 내용인 문자열(String) 객체를 반환합니다.
        """

        # compile() 메서드를 사용해 삭제할 괄호 및 괄호 안의 문자, 이메일, 기자 이름을 지정
        parenthesize_pattern = re.compile(r"\[[^]]*\]|\([^)]*\)|\<[^>]*\>")
        email_pattern = re.compile(r"[a-zA-Z0-9+-_./]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
        reporter_pattern = re.compile(r"([가-힣]{2,5} 기자)|([가-힣]{2,5}기자)")

        # sub() 메서드를 사용해 괄호 및 괄호 안의 문자, 이메일, 기자 이름을 제거
        news_body = parenthesize_pattern.sub("", news_body).strip()
        news_body = email_pattern.sub("", news_body).strip()
        news_body = reporter_pattern.sub("", news_body).strip()

        # replace() 메서드를 사용해 줄바꿈, 탭, 띄워쓰기 제거
        news_body = news_body.replace("\n", " ").replace("\t", " ").replace("\xa0", " ")

        # translate() 메서드를 사용해 각종 기호, 줄바꿈 및 문장부호를 제거
        symbols = string.punctuation.replace("%", "").replace("~", "") + "·ㆍ■◆△▷▶▼�“”‘’…※↑↓"
        news_body = news_body.translate(str.maketrans("", "", symbols))

        # 결과 값 반환
        return news_body