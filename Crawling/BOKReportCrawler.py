# 필요한 모듈 불러오기
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# ----------------------------------------------------------------------------------------------------

# crawling_link_loader() 함수 정의
def crawling_link_loader() -> tuple:
    """
    내부 데이터의 사업자등록번호를 불러와 검색하고 그 결과 웹 페이지의 주소를 가져오는 함수입니다.\n
    웹 페이지 주소가 담긴 리스트와 웹 페이지 주소가 없는 사업자등록번호가 담긴 리스트를 튜플로 묶어 반환합니다.
    """

    # 개업 중소기업과 휴·폐업 중소기업 데이터를 불러와 각 데이터프레임으로 변환
    active_df = pd.read_excel("./../Data/1_Active_MS_Business_Info.xlsx", sheet_name = 0)
    inactive_df = pd.read_excel("./../Data/2_Inactive_MS_Business_Info.xlsx", sheet_name = 0)

    # concat() 함수를 사용해 각 데이터프레임에서 사업자등록번호만을 불러와 데이터프레임 biz_num_df 생성
    biz_num_df = pd.concat([active_df["BIZ_NO"], inactive_df["BIZ_NO"]], ignore_index = True)

    # 불러온 사업자등록번호의 개수를 안내하는 메시지를 출력
    print(">>> 내부 데이터로부터 총 {}개의 사업자등록번호를 가져왔습니다.".format(len(biz_num_df)))

    # 웹 페이지 주소 크롤링의 시작을 안내하는 메시지를 출력
    print(">>> 각 사업자등록번호를 이용해 웹 페이지 주소 크롤링을 시작합니다.")

    # 웹 페이지 주소를 크롤링한 결과를 담을 리스트 초기화
    valid_list = []
    nonvalid_list = []

    # 웹 페이지 연결에 필요한 헤더 옵션을 딕셔너리 headers에 할당
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
    }

    # for 반복문을 통해 biz_num_df의 각 사업자등록번호를 순회
    for idx, biz_num in enumerate(biz_num_df):
        try:

            # GET 요청을 보내고 응답 코드를 출력
            res = requests.get(f"https://www.nicebizinfo.com/ep/EP0100M001GE.nice?itgSrch={biz_num}", headers = headers)
            print(">>> ({0}) 사업자등록번호 [{1}]의 응답 코드: {2}".format(idx + 1, biz_num, res.status_code))

            # 해당 웹 페이지의 HTML 코드 텍스트를 가져와 변수 soup에 할당
            soup = BeautifulSoup(res.text, "html.parser")

            # HTML 코드에서 웹 페이지 주소 쿼리 스트링(Query String)에 사용할 kiscode 추출
            kiscode = soup.select_one(".cTable input").attrs["value"]
        
            # 추출한 kiscode를 사용해 웹 페이지 주소를 리스트 valid_list에 추가 및 메시지 출력
            valid_dict = {
                "biz_num": biz_num,
                "web_address": f"https://www.nicebizinfo.com/ep/EP0100M002GE.nice?kiscode={kiscode}"
            }
            valid_list.append(valid_dict)
            print(f">>> ({idx + 1}) 사업자등록번호 [{biz_num}]의 웹 페이지 주소가 기록되었습니다.")

        # 오류가 발생한 경우 해당 오류를 출력 후 오류가 발생한 사업자등록번호를 nonvalid_list에 추가
        except Exception as e:
            print(">>> ({0}) 사업자등록번호 [{1}]에서 다음의 오류가 발생했습니다: {2}".format(idx + 1, biz_num, e))
            nonvalid_list.append(biz_num)

    # 웹 페이지 주소 크롤링의 끝을 안내하는 메시지를 출력
    print(">>> 각 사업자등록번호를 이용한 웹 페이지 주소 크롤링이 완료되었습니다.")

    # 결과 값 반환
    return valid_list, nonvalid_list

# ----------------------------------------------------------------------------------------------------

# info_crawler() 함수 정의
def info_crawler(valid_list : list) -> tuple:
    """
    웹 페이지 주소를 불러와 본사 주소, 산업평가 종합등급, 산업의 3개년 평균 매출액을 크롤링하는 함수입니다.\n
    사업자등록번호 및 크롤링한 정보가 담긴 리스트와 필요한 정보가 없는 사업자등록번호 리스트를 튜플로 묶어 반환합니다.
    """

    # 불러온 사업자등록번호의 개수를 안내하는 메시지를 출력
    print(">>> 사업자등록번호로 총 {}개의 유효한 웹 페이지 주소를 가져왔습니다.".format(len(valid_list)))

    # 크롤링의 시작을 안내하는 메시지를 출력
    print(">>> 각 웹 페이지 주소에서 크롤링을 시작합니다.")

    # 각 정보를 크롤링한 결과를 담을 리스트 초기화
    result = []
    no_info_list = []

    # 웹 페이지 연결에 필요한 헤더 옵션을 딕셔너리 headers에 할당
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
    }

    # for 반복문을 통해 valid_list의 각 딕셔너리를 순회
    for idx, valid in enumerate(valid_list):

        # GET 요청을 보내고 응답 코드를 출력
        res = requests.get(valid["web_address"], headers = headers, stream = True)
        print(">>> ({0}) 사업자등록번호 [{1}]의 응답 코드: {2}".format(idx + 1, valid["biz_num"], res.status_code))

        # 차단을 방지하기 위해 sleep() 함수로 2초 대기
        time.sleep(2)
        
        # 해당 웹 페이지의 HTML 코드 텍스트를 가져와 변수 soup에 할당
        soup = BeautifulSoup(res.text, "html.parser")

        # HTML 코드에서 본사 주소, 산업평가 종합등급, 3개년 평균 매출액 정보를 추출
        try:
            address = soup.select_one(".iconBox.bg2 strong").text.strip()
            grade = soup.select(".cTable.sp2")[1].select("tr td")[3].text.strip()
            avg_sales_2019 = soup.select(".cTable.sp8.mb10 td.tar")[2].text.replace(",", "").strip()
            avg_sales_2020 = soup.select(".cTable.sp8.mb10 td.tar")[1].text.replace(",", "").strip()
            avg_sales_2021 = soup.select(".cTable.sp8.mb10 td.tar")[0].text.replace(",", "").strip()
            
            # 추출한 정보를 리스트 result에 추가 및 메시지 출력
            info_dict = {
                "biz_num": valid["biz_num"],
                "address": address,
                "grade": grade,
                "avg_sales_2019": avg_sales_2019,
                "avg_sales_2020": avg_sales_2020,
                "avg_sales_2021": avg_sales_2021
            }
            result.append(info_dict)
            print(">>> ({0}) 사업자등록번호 [{1}]의 정보를 성공적으로 추출했습니다.".format(idx + 1, valid["biz_num"]))

        # 오류가 발생한 경우 해당 오류를 출력 후 오류가 발생한 사업자등록번호를 no_info_list에 추가
        except Exception as e:
            print(">>> ({0}) 사업자등록번호 [{1}]에서 다음의 오류가 발생했습니다: {2}".format(idx + 1, valid["biz_num"], e))
            no_info_list.append(valid["biz_num"])

    # 크롤링의 끝을 안내하는 메시지를 출력
    print(">>> 각 웹 페이지 주소를 이용한 크롤링이 완료되었습니다.")

    # 결과 값 반환
    return result, no_info_list