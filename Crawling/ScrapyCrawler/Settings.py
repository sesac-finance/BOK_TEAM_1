# scrapy 프로젝트에서 실행되는 봇 이름 설정
BOT_NAME = "ScrapyCrawler"

# scrapy에서 스파이더(Spider)를 찾을 모듈의 목록을 리스트로 저장
SPIDER_MODULES = ["ScrapyCrawler.spiders"]

# genspider 명령어를 사용해 새로운 스파이더를 생성할 위치를 지정
NEWSPIDER_MODULE = "ScrapyCrawler.spiders"

# robots.txt 정책을 존중할지 설정
ROBOTSTXT_OBEY = False

# 크롤링 결과를 내보낼 파일의 이름, 형식, 인코딩 설정
FEEDS = {
    "./../Data/bond_report_locator.csv": {
        "format": "csv",
        "encoding": "utf-8-sig"
    }
}

# 웹 사이트에서 각 값을 다운로드를 위해 대기할 시간을 설정
DOWNLOAD_DELAY = 0.15