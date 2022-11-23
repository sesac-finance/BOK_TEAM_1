# 필요한 모듈 불러오기
from scrapy.exporters import CsvItemExporter
import pandas as pd
from scrapy.exceptions import DropItem

# MultiCSVItemPipeline() 클래스 정의
class MultiCSVItemPipeline(object):

    # 저장할 파일 이름을 담은 리스트 save_names 초기화
    save_names = ["MPB_Minute_Location", "Bond_Report_Location", "News_Location", "News_Content"]

    # 아이템의 개수를 셀 변수 item_cnt 초기화
    item_cnt = 0

    # ----------------------------------------------------------------------------------------------------

    # open_spider() 함수 정의
    def open_spider(self, spider):
        """
        스파이더(Spider)가 열릴 때 실행되는 함수입니다.\n
        크롤링한 데이터를 CSV 파일로 내보내는 과정을 시작합니다.
        """

        # Spider 클래스의 객체 이름에 따라 save_names의 각 값을 변수 spider_type에 할당
        if spider.name == "MPBMinuteCrawler": spider_type = self.save_names[0]
        elif spider.name == "BondReportCrawler": spider_type = self.save_names[1]
        elif spider.name == "NewsURLCrawler": spider_type = self.save_names[2]
        else: spider_type = self.save_names[3]

        # 뉴스 내용을 가져오는 경우 뉴스 URL을 담은 CSV 파일을 불러와 데이터프레임 생성
        if spider.name == "NewsContentCrawler":
            news_df = pd.read_csv("./../Data/News_Location.csv", header = 0, encoding = "utf-8-sig")

            # CSV 파일의 행 개수에 따라 50,000개 데이터가 담길 수 있도록 CSV 파일을 여러 개 생성 후 내보내기 시작
            self.files = dict([(file_num, open(f"./../Data/{spider_type}_{file_num}.csv", "wb")) for file_num in range(1, len(news_df) // 50000 + 2)])
            self.exporters = dict([(file_num, CsvItemExporter(self.files[file_num], encoding = "utf-8-sig")) for file_num in range(1, len(news_df) // 50000 + 2)])
            [csv_exporter.start_exporting() for csv_exporter in self.exporters.values()]

        else:
            # 지정한 파일 이름의 CSV 파일을 생성 후 열기
            self.file = open(f"./../Data/{spider_type}.csv", "wb")

            # CSV 파일로 내보내기 시작
            self.exporter = CsvItemExporter(self.file, encoding = "utf-8-sig")
            self.exporter.start_exporting()

    # ----------------------------------------------------------------------------------------------------

    # process_item() 함수 정의
    def process_item(self, item, spider):
        """
       파이프라인 과정의 모든 Item 클래스 객체에 대해 실행되는 함수입니다.\n
        scrapy 모듈의 Item 클래스 객체를 반환합니다.
        """

        # 값이 존재하지 않는 경우 파이프라인에서 해당 객체 제거 후 오류 메시지 출력
        if not all(item.values()):
            print(">>> 다음과 같이 크롤링한 데이터가 존재하지 않습니다: ", item)
            raise DropItem()

        # 수집 기간과 일치하지 않는 경우 파이프라인에서 해당 객체 제거 후 오류 메시지 출력
        year = int(item["date"][: 4])
        month = int(item["date"][5 : 7])
        if year < 2012 or (year == 2012 and month < 11) or (year == 2022 and month > 10):
            print(">>> 크롤링한 데이터의 날짜가 수집 기간과 일치하지 않습니다: ", item["date"])
            raise DropItem()

        # 뉴스 내용을 가져오는 경우 50,000개씩 데이터를 나누어 저장
        if spider.name == "NewsContentCrawler":
            self.item_cnt += 1
            list(self.exporters.values())[self.item_cnt // 50000].export_item(item)

        else:
            # Item 객체를 CSV 파일로 내보내기
            self.exporter.export_item(item)

        # 결과 값 반환
        return item

    # ----------------------------------------------------------------------------------------------------

    # close_spider() 함수 정의
    def close_spider(self, spider):
        """
        스파이더(Spider)가 종료될 때 실행되는 함수입니다.\n
        크롤링한 데이터를 CSV 파일로 내보내는 과정을 종료합니다.
        """

        # 뉴스 내용을 가져오는 경우 여러 파일에 대해 내보내기 및 파일 종료
        if spider.name == "NewsContentCrawler":
            [csv_exporter.finish_exporting() for csv_exporter in self.exporters.values()]
            [csv_file.close() for csv_file in self.files.values()]

        else:
            # CSV 파일로 내보내기 종료
            self.exporter.finish_exporting()

            # CSV 파일 종료
            self.file.close()