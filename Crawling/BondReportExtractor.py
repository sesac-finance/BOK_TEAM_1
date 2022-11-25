# 필요한 모듈 불러오기
import pandas as pd
from tika import parser
import os
import requests
import re
import string

# ----------------------------------------------------------------------------------------------------

# 추출할 PDF 파일과 추출한 텍스트를 담을 CSV 파일을 저장할 경로를 상수 SAVE_DIR에 할당
SAVE_DIR = "./Data/Text/"

# ----------------------------------------------------------------------------------------------------

# bond_report_extractor() 함수 정의
def bond_report_extractor():
    """
    다운로드 받은 PDF 파일에서 텍스트를 추출하고 이를 CSV 파일로 저장하는 함수입니다.
    """

    # 추출한 텍스트를 저장할 데이터프레임 report_df 생성
    report_df = pd.DataFrame(columns = ["date", "title", "content"])

    # 다운로드 받은 PDF 파일의 정보를 담은 딕셔너리를 bond_reports에 할당
    bond_reports = bond_report_downloader()

    # 텍스트 추출의 시작을 알리는 안내 메시지 출력
    print(">>> 채권 리포트 텍스트 추출이 시작되었습니다.\n")


    # for 반복문 및 enumerate를 사용해 채권 리포트의 제목과 인덱스를 순회
    for idx, pdf_title in enumerate(bond_reports.keys()):

        # from_file() 함수를 사용해 리포트 PDF 파일을 가져와 텍스트 추출
        parsed_pdf = parser.from_file(SAVE_DIR + f"{pdf_title}.pdf")
        texts = parsed_pdf["content"].strip().split("\n")

        # 리포트 PDF 파일에서 추출한 텍스트를 전처리하여 할당 후 안내 메시지 출력
        report_df.loc[idx] = [list(bond_reports.values())[idx], pdf_title, bond_report_cleanser(" ".join(texts))]
        print(f">>> 다음 채권 리포트 텍스트 추출이 완료되었습니다: {pdf_title}.pdf\n")

        # 텍스트 추출이 끝난 PDF 파일을 삭제
        os.remove(SAVE_DIR + f"{pdf_title}.pdf")
    
    # 추출한 결과를 CSV 파일로 저장
    report_df.to_csv(SAVE_DIR + "Bond_Report_Content.csv", index = False)

    # 텍스트 추출의 끝을 알리는 안내 메시지 출력
    print(">>> 채권 리포트 텍스트 추출이 완료되었습니다.")

# ----------------------------------------------------------------------------------------------------

# bond_report_downloader() 함수 정의
def bond_report_downloader() -> dict :
    """
    Bond_Report_Location.csv 파일에서 PDF 파일의 주소를 가져와 다운로드하는 함수입니다.\n
    채권 리포트의 작성일자를 키(Key), 제목을 값(Value)으로 하는 딕셔너리를 반환합니다.
    """

    # 채권 리포트의 제목과 작성일자를 담을 딕셔너리를 초기화
    bond_reports = {}

    # PDF 파일의 주소를 담은 CSV 파일을 가져와 데이터프레임 report_url_df 생성
    report_url_df = pd.read_csv("./Data/Location/Bond_Report_Location.csv", header = 0, encoding = "utf-8-sig")

    # 파일을 저장할 경로가 존재하지 않는 경우 mkdir() 함수를 사용해 경로 생성
    if not os.path.isdir(SAVE_DIR):
        os.mkdir(SAVE_DIR)

    # 다운로드 시작을 알리는 안내 메시지 출력
    print(">>> 채권 리포트 PDF 파일 다운로드를 시작합니다.\n")

    # for 반복문을 사용하여 데이터프레임의 각 행을 순회
    for idx in report_url_df.index:

        # 채권 리포트의 URL, 전처리한 제목, 증권사, 작성일자를 추출
        title = report_url_df.loc[idx, "title"].translate(str.maketrans("", "", string.punctuation)).strip().replace(" ", "_")
        writer = report_url_df.loc[idx, "writer"].strip()
        file_url = report_url_df.loc[idx, "file_url"]
        date = report_url_df.loc[idx, "date"]

        # 채권 리포트의 증권사-제목을 키(Key), 작성일자를 값(Value)으로 딕셔너리에 추가
        bond_reports["{0}-{1}-{2}".format(idx, writer, title)] = date

        # 채권 리포트 증권사-제목으로 PDF 파일을 다운로드
        with open(SAVE_DIR + "{0}.pdf".format(f"{idx}-{writer}-{title}"), "wb") as file:

            # GET 요청을 보내고 응답 코드를 출력
            res = requests.get(file_url)
            print(">>> 채권 리포트 파일의 응답 코드: {0}\n".format(res.status_code))

            # PDF 파일을 저장
            file.write(res.content)

    # 다운로드 끝을 알리는 안내 메시지 출력
    print(">>> 채권 리포트 PDF 파일 다운로드가 완료되었습니다.\n")

    # 결과 값 반환
    return bond_reports

# ----------------------------------------------------------------------------------------------------

# bond_report_cleanser() 함수 정의
def bond_report_cleanser(text : str) -> str :
    """
    채권 리포트 내용을 가져와 문장 부호와 불필요한 내용을 제거하는 함수입니다.\n
    전처리된 리포트 내용인 문자열(String) 객체를 반환합니다.
    """

    # compile() 메서드를 사용해 삭제할 괄호 및 괄호 안의 문자, 페이지를 지정
    parenthesize_pattern = re.compile(r"\[[^]]*\]|\([^)]*\)|\<[^>]*\>|\〈[^〉]*\〉")
    email_pattern = re.compile(r"[a-zA-Z0-9+-_./]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    page_pattern = re.compile(r"\- [0-9]{1,3} \-")

    # sub() 메서드를 사용해 괄호 및 괄호 안의 문자, 페이지를 제거
    text = parenthesize_pattern.sub("", text).strip()
    text = email_pattern.sub("", text).strip()
    text = page_pattern.sub("", text).strip()

    # replace() 메서드를 사용해 줄바꿈, 탭, 띄워쓰기 제거
    text = text.replace("\n", "").replace("\t", "")

    # translate() 메서드를 사용해 각종 기호, 줄바꿈 및 문장부호를 제거
    symbols = string.punctuation.replace("%", "").replace("~", "") + "·･ㆍ․▣■□◆△▶▷“”‘’…※↑↓「」｢｣ⅠⅡⅢ�①②③④⑤"
    text = text.translate(str.maketrans("", "", symbols)).strip()

    # 결과 값 반환
    return text