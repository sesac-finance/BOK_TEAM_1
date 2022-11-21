# 필요한 모듈 불러오기
from scrapy.exporters import CsvItemExporter
from scrapy.exceptions import DropItem

# MultiCSVItemPipeline() 클래스 정의
class MultiCSVItemPipeline(object):

    # 저장할 파일 이름을 담은 리스트 save_names 초기화
    save_names = ["MPB_Minute_Location", "Bond_Report_Location"]

    # ----------------------------------------------------------------------------------------------------

    # open_spider() 함수 정의
    def open_spider(self, spider):
        """
        스파이더(Spider)가 열릴 때 실행되는 함수입니다.\n
        크롤링한 데이터를 CSV 파일로 내보내는 과정을 시작합니다.
        """

        # Spider 클래스의 객체 이름에 따라 save_names의 각 값을 변수 spider_type에 할당
        if spider.name == "MPBMinuteCrawler":
            spider_type = self.save_names[0]
        else:
            spider_type = self.save_names[1]

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

        if int(item["date"][0][: 4]) < 2008:
            print(">>> 크롤링한 데이터의 날짜가 수집 기간과 일치하지 않습니다: ", item["date"][0])
            raise DropItem()

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

        # CSV 파일로 내보내기 종료
        self.exporter.finish_exporting()

        # CSV 파일 종료
        self.file.close()