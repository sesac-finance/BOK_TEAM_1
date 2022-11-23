# 필요한 모듈 불러오기
from tika import parser
import pandas as pd
import os
import requests

# ----------------------------------------------------------------------------------------------------

# 추출할 PDF 파일과 추출한 텍스트를 담을 CSV 파일을 저장할 경로를 상수 SAVE_DIR에 할당
SAVE_DIR = "./Data/Text/"

# ----------------------------------------------------------------------------------------------------

# mpb_minute_extractor() 함수 정의
def mpb_minute_extractor():
    """
    다운로드 받은 PDF 파일에서 텍스트를 추출하고 이를 CSV 파일로 저장하는 함수입니다.
    """

    # 다운로드 받은 PDF 파일의 정보를 담은 딕셔너리를 mpb_minutes에 할당
    mpb_minutes = mpb_minute_downloader()

    # for 반복문을 사용해 금융통화위원회 의사록의 제목을 순회
    for pdf_title in mpb_minutes.values():

        # from_file() 함수를 사용해 의사록 PDF 파일을 가져와 텍스트 추출
        parsed_pdf = parser.from_file(SAVE_DIR + f"{pdf_title}.pdf")
        texts = parsed_pdf["content"].strip()

        print(texts)

# ----------------------------------------------------------------------------------------------------

# mpb_minute_downloader() 함수 정의
def mpb_minute_downloader() -> dict :
    """
    MPB_Minute_Location.csv 파일에서 PDF 파일의 주소를 가져와 다운로드하는 함수입니다.\n
    금융통화위원회 의사록의 작성일자를 키(Key), 제목을 값(Value)으로 하는 딕셔너리를 반환합니다.
    """

    # 금융통화위원회 의사록의 제목과 작성일자를 담을 딕셔너리를 초기화
    mpb_minutes = {}

    # PDF 파일의 주소를 담은 CSV 파일을 가져와 데이터프레임 minute_url_df 생성
    minute_url_df = pd.read_csv("./Data/Location/MPB_Minute_Location.csv", header = 0, encoding = "utf-8-sig")

    # 파일을 저장할 경로가 존재하지 않는 경우 mkdir() 함수를 사용해 경로 생성
    if not os.path.isdir(SAVE_DIR):
        os.mkdir(SAVE_DIR)

    # 다운로드 시작을 알리는 안내 메시지 출력
    print(">>> 금융통화위원회 의사록 PDF 파일 다운로드를 시작합니다.\n")

    # for 반복문을 사용하여 데이터프레임의 각 행을 순회
    for idx in minute_url_df.index:

        # 금융통화위원회 의사록의 URL, 제목, 작성일자를 추출
        title = minute_url_df.loc[idx, "title"]
        file_url = minute_url_df.loc[idx, "file_url"]
        date = minute_url_df.loc[idx, "date"]

        # 금융통화위원회 의사록의 작성일자를 키(Key), 제목을 값(Value)으로 딕셔너리에 추가
        mpb_minutes[date] = title

        # 금융통화위원회의 의사록 제목으로 PDF 파일을 다운로드
        with open(SAVE_DIR + "{0}.pdf".format(title), "wb") as file:

            # GET 요청을 보내고 응답 코드를 출력
            res = requests.get(file_url)
            print(">>> 금융통화위원회 의사록 파일의 응답 코드:  {0}\n".format(res.status_code))

            # PDF 파일을 저장
            file.write(res.content)

    # 다운로드 끝을 알리는 안내 메시지 출력
    print(">>> 금융통화위원회 의사록 PDF 파일 다운로드가 완료되었습니다.\n")

    # 결과 값 반환
    return mpb_minutes

# ----------------------------------------------------------------------------------------------------

# pdf에 있는 텍스트를 추출
def extract_text():
    pdf_dir = "./../Data/pdf_data"
    pdf_list = os.listdir(pdf_dir)

    for pdf in pdf_list:
        # print(pdf)
        # print(pdf[:-4])
        path = f"./../Data/pdf_data/{pdf}"
        parsed = parser.from_file(path)
        txt_dir = f"./../Data/TXT/{pdf[:-4]}.txt"
        txt = open(txt_dir, 'w', encoding='utf-8-sig')
        print(parsed['content'], file=txt)
        txt.close()

mpb_minute_extractor()