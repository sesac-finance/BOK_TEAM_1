# 필요한 모듈 불러오기
import requests
from bs4 import BeautifulSoup
import pandas as pd

# ----------------------------------------------------------------------------------------------------

# 쿼리 스트링(Query String)을 제외한 기본 URL을 base_url에 할당
base_url = "https://www.bok.or.kr/portal/singl/baseRate/list.do"

# ----------------------------------------------------------------------------------------------------

# base_rate_crawler() 함수 정의
def base_rate_crawler() -> list:
    """
    한국은행 기준금리 추이 웹 페이지에서 기준금리를 크롤링하는 함수입니다.\n
    기준금리 변경일자와 기준금리가 담긴 리스트로 구성된 리스트를 반환합니다.
    """

    # 기준금리 크롤링의 시작을 안내하는 메시지를 출력
    print(">>> 한국은행 기준금리 추이 웹 페이지에서 기준금리 크롤링을 시작합니다.")

    # 기준금리를 크롤링한 결과를 담을 리스트 interest_result 초기화
    interest_result = []

    # 쿼리 스트링(Query String)을 query_url에 할당
    query_url = "?dataSeCd=01&menuNo=200643"

    # GET 요청을 보내고 응답 코드를 출력
    res = requests.get(base_url + query_url)
    print(">>> 한국은행 기준금리 추이 웹 페이지의 응답 코드:  {0}\n".format(res.status_code))

    # 해당 웹 페이지의 HTML 코드 텍스트를 가져와 변수 soup에 할당
    soup = BeautifulSoup(res.text, "html.parser")

    # for 반복문을 통해 표의 각 행을 순회
    for row in soup.select(".fixed tbody tr"):

        # 각 행에서 연도, 월, 일, 기준금리를 추출해 각 변수에 할당
        year = int(row.select("td")[0].text)
        month = int(row.select("td")[1].text.split(" ")[0].replace("월", ""))
        day = int(row.select("td")[1].text.split(" ")[1].replace("일", ""))
        base_rate = row.select("td")[2].text

        # 연도가 2007년 이전이거나 2007년이면서 8월 이전의 데이터는 제외
        if year < 2007 or (year == 2007 and month < 8):
            continue

        # 리스트 interest_result에 날짜와 기준금리로 구성된 리스트를 추가
        interest_result.append([f"{year}.{month}.{day}", base_rate])
    
    # 기준금리 크롤링의 끝을 안내하는 메시지를 출력
    print(">>> 한국은행 기준금리 추이 웹 페이지에서 기준금리 크롤링이 완료되었습니다.")
    
    # 결과 값 반환
    return interest_result

# ----------------------------------------------------------------------------------------------------

# base_rate_exporter() 함수 정의
def base_rate_exporter(interest_result : list):
    """
    크롤링한 결과를 입력 받아 CSV 파일로 저장하는 함수입니다.
    """

    # 기준금리 크롤링 결과를 사용해 데이터프레임 interest_df 생성
    base_rate_df = pd.DataFrame(interest_result, columns = ["date", "interest_rate"])

    # to_csv() 메서드를 사용해 데이터프레임을 CSV 파일로 저장 후 안내 메시지 출력
    base_rate_df.to_csv("./../Data/Base_Rate.csv", index = False, encoding = "utf-8-sig")
    print(">>> 한국은행 기준금리 크롤링 결과가 저장되었습니다.")