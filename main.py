import os
import json
import boto3
import logging
import time
import re
import shutil
import stat
import subprocess
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

# Lambda 환경을 위한 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 클라이언트 설정
s3_client = boto3.client('s3')

# 환경 변수 설정
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'exchange-rate-crawler-bucket')

# Lambda Layer에서 Chrome과 ChromeDriver 경로
# 일반적으로 사용되는 여러 가능한 경로를 시도
CHROME_PATHS = [
    '/opt/chrome/chrome',
    '/opt/chrome/headless-chromium',
    '/opt/chrome-linux/chrome',
    '/opt/headless-chromium',
    '/var/task/headless-chromium'
]

CHROMEDRIVER_PATHS = [
    '/opt/chromedriver',
    '/opt/chrome/chromedriver',
    '/var/task/chromedriver'
]

def check_environment():
    """환경 확인을 위한 함수"""
    logger.info("Lambda 환경 확인 중...")
    
    # 디렉토리 목록 확인
    try:
        logger.info(f"/opt 디렉토리 내용: {os.listdir('/opt')}")
    except Exception as e:
        logger.error(f"/opt 디렉토리 확인 실패: {e}")
    
    # Chrome 경로 확인
    chrome_path = None
    for path in CHROME_PATHS:
        if os.path.exists(path):
            logger.info(f"Chrome 발견: {path}")
            chrome_path = path
            logger.info(f"Chrome 권한: {oct(os.stat(path).st_mode)}")
            
            # Chrome 버전 확인 시도
            try:
                result = subprocess.run([path, "--version"], capture_output=True, text=True)
                logger.info(f"Chrome 버전: {result.stdout}")
            except Exception as e:
                logger.error(f"Chrome 버전 확인 실패: {e}")
            break
    
    if not chrome_path:
        logger.warning("Chrome을 찾을 수 없습니다.")
    
    # ChromeDriver 경로 확인
    driver_path = None
    for path in CHROMEDRIVER_PATHS:
        if os.path.exists(path):
            logger.info(f"ChromeDriver 발견: {path}")
            driver_path = path
            logger.info(f"ChromeDriver 권한: {oct(os.stat(path).st_mode)}")
            
            # ChromeDriver 버전 확인 시도
            try:
                result = subprocess.run([path, "--version"], capture_output=True, text=True)
                logger.info(f"ChromeDriver 버전: {result.stdout}")
            except Exception as e:
                logger.error(f"ChromeDriver 버전 확인 실패: {e}")
            break
    
    if not driver_path:
        logger.warning("ChromeDriver를 찾을 수 없습니다.")
    
    # 환경 변수 확인
    logger.info(f"PATH 환경 변수: {os.environ.get('PATH', '')}")
    
    return chrome_path, driver_path

class DaumExchangeRateCrawler:
    def __init__(self):
        """
        Daum 금융 환율 크롤러 초기화 (Lambda 환경용)
        """
        self.base_url = "https://finance.daum.net/exchanges/FRX.KRWUSD"
        self.driver = None
        self.chrome_path, self.driver_path = check_environment()
        
        # 임시 디렉토리에 ChromeDriver 복사
        if self.driver_path:
            self.tmp_driver_path = '/tmp/chromedriver'
            try:
                shutil.copy2(self.driver_path, self.tmp_driver_path)
                os.chmod(self.tmp_driver_path, stat.S_IRWXU)  # 0o700 (rwx)
                logger.info(f"ChromeDriver를 임시 디렉토리에 복사: {self.tmp_driver_path}")
                self.driver_path = self.tmp_driver_path
            except Exception as e:
                logger.error(f"ChromeDriver 복사 실패: {e}")
    
    def setup_driver(self):
        """Lambda 환경에 맞게 Selenium WebDriver 설정 (Selenium 4.x 호환)"""
        chrome_options = Options()
        
        if self.chrome_path:
            chrome_options.binary_location = self.chrome_path
        
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--single-process")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--hide-scrollbars")
        chrome_options.add_argument("--enable-logging")
        chrome_options.add_argument("--log-level=0")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--homedir=/tmp")
        chrome_options.add_argument("--disk-cache-dir=/tmp/cache-dir")
        
        # User-Agent 설정
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            # Selenium 4.x 버전에서는 Service 객체를 사용해야 함
            from selenium.webdriver.chrome.service import Service
            
            if self.driver_path:
                # Service 객체 생성
                service = Service(executable_path=self.driver_path)
                
                # WebDriver 생성 (명시적 경로 사용)
                self.driver = webdriver.Chrome(
                    service=service,
                    options=chrome_options
                )
                logger.info(f"WebDriver 설정 완료 (명시적 경로 사용: {self.driver_path})")
            else:
                # 자동 감지 사용
                self.driver = webdriver.Chrome(options=chrome_options)
                logger.info("WebDriver 설정 완료 (자동 감지 사용)")
        except Exception as e:
            logger.error(f"WebDriver 설정 실패: {e}")
            # Selenium 버전 확인
            import selenium
            logger.error(f"Selenium 버전: {selenium.__version__}")
            
            # 대체 방법 시도
            try:
                logger.info("대체 방법으로 WebDriver 설정 시도")
                self.driver = webdriver.Chrome(options=chrome_options)
                logger.info("대체 방법으로 WebDriver 설정 완료")
            except Exception as e2:
                logger.error(f"대체 방법 실패: {e2}")
                raise
    
    def go_to_last_page(self):
        """마지막 페이지로 이동"""
        try:
            # 페이지 로드 기다리기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # 시도 1: 직접 XPath로 마지막 페이지 버튼 찾기
            try:
                last_page_button_xpath = "/html/body/div/div[4]/div/div[4]/div[3]/div[2]/div/div/a[11]"
                last_page_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, last_page_button_xpath))
                )
                
                # 버튼 텍스트 확인
                logger.info(f"마지막 페이지 버튼 텍스트: {last_page_button.text}")
                
                # JavaScript로 버튼 클릭
                self.driver.execute_script("arguments[0].click();", last_page_button)
                logger.info("마지막 페이지 버튼 클릭됨 (방법 1)")
                
                # 페이지 로드 기다리기
                time.sleep(2)
                return True
            except (TimeoutException, NoSuchElementException) as e:
                logger.warning(f"방법 1 실패: {e}")
            
            # 시도 2: 텍스트 내용으로 마지막 버튼 찾기
            try:
                # 모든 페이지네이션 링크 가져오기
                pagination_links = self.driver.find_elements(By.XPATH, 
                    "//div[contains(@class, 'tableUI-navigate')]/a | //div[contains(@class, 'pagination')]/a")
                
                last_button = None
                for link in pagination_links:
                    if link.text.strip() in ["마지막", "끝", ">>", ">|", "Last"]:
                        last_button = link
                        break
                
                if last_button:
                    self.driver.execute_script("arguments[0].click();", last_button)
                    logger.info(f"마지막 페이지 버튼 클릭됨 (방법 2): {last_button.text}")
                    time.sleep(2)
                    return True
                else:
                    logger.warning("마지막 페이지 텍스트 버튼을 찾을 수 없습니다.")
            except Exception as e:
                logger.warning(f"방법 2 실패: {e}")
            
            # 시도 3: 자바스크립트로 직접 페이지 요청
            try:
                # 페이지 요소 확인
                page_links = self.driver.find_elements(By.XPATH, 
                    "//div[contains(@class, 'tableUI-navigate')]/a[string-length(text()) = 1 or string-length(text()) = 2]")
                
                # 숫자 버튼들 중 가장 큰 숫자 찾기
                highest_page = 0
                for link in page_links:
                    try:
                        page_num = int(link.text.strip())
                        if page_num > highest_page:
                            highest_page = page_num
                    except ValueError:
                        continue
                
                if highest_page > 0:
                    # 가장 높은 페이지 번호 + 10을 마지막 페이지로 추정하여 직접 이동
                    estimated_last_page = highest_page + 10
                    logger.info(f"자바스크립트로 직접 페이지 {estimated_last_page}로 이동 시도")
                    
                    # URL에 페이지 파라미터 추가 시도
                    self.driver.execute_script(f"window.location.href = '{self.base_url}?page={estimated_last_page}'")
                    time.sleep(3)
                    return True
            except Exception as e:
                logger.warning(f"방법 3 실패: {e}")
            
            return False
        except Exception as e:
            logger.error(f"마지막 페이지로 이동 중 오류 발생: {e}")
            return False
    
    def extract_exchange_data(self):
        """현재 페이지에서 환율 데이터 추출"""
        try:
            # 테이블 로드 기다리기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # 여러 가능한 테이블 선택자 시도
            selectors = [
                "//table/tbody/tr",
                "//table//tr",
                "//div[contains(@class, 'tableUI')]//table//tr",
                "/html/body/div/div[4]/div/div[4]/div[3]/div[2]/div/table/tbody/tr"
            ]
            
            rows = []
            for selector in selectors:
                try:
                    # 테이블 로드 기다리기
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    
                    # 모든 행 가져오기
                    rows = self.driver.find_elements(By.XPATH, selector)
                    if rows:
                        logger.info(f"선택자 '{selector}'로 {len(rows)}개의 행을 찾았습니다.")
                        break
                except (TimeoutException, NoSuchElementException):
                    logger.warning(f"선택자 '{selector}'로 행을 찾을 수 없습니다.")
                    continue
                    
            if not rows:
                # 더 기본적인 선택자로 다시 시도
                rows = self.driver.find_elements(By.TAG_NAME, "tr")
                logger.info(f"기본 'tr' 태그로 {len(rows)}개의 행을 찾았습니다.")
            
            logger.info(f"테이블에서 {len(rows)}개의 행을 찾았습니다.")
            
            data = []
            for idx, row in enumerate(rows):
                try:
                    # 모든 td 요소 가져오기
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    # 셀이 없거나 충분하지 않으면 th 요소도 확인
                    if len(cells) < 2:
                        cells = row.find_elements(By.TAG_NAME, "th")
                    
                    # 충분한 셀이 있는지 확인
                    if len(cells) >= 2:
                        # 첫 번째 셀은 고시회차
                        session = cells[0].text.strip()
                        
                        # 가격 정보가 있는 셀 찾기 (여러 위치 시도)
                        rate = ""
                        potential_rate_cells = [2, 1] if len(cells) >= 3 else [1]
                        
                        for cell_idx in potential_rate_cells:
                            try:
                                cell_text = cells[cell_idx].text.strip()
                                # 숫자와 특수문자(소수점, 쉼표 등)가 포함된 텍스트인지 확인
                                if any(c.isdigit() for c in cell_text):
                                    rate = cell_text
                                    break
                            except (IndexError, AttributeError):
                                continue
                        
                        if session and rate:
                            data.append({
                                "고시회차": session,
                                "현재가 환율": rate,
                                "수집일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                            logger.info(f"수집 데이터: 고시회차={session}, 현재가={rate}")
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    logger.error(f"행 {idx} 데이터 추출 중 오류: {e}")
            
            return data
            
        except Exception as e:
            logger.error(f"데이터 추출 중 오류 발생: {e}")
            return []
    
    def find_specific_session(self, session_number="1회"):
        """특정 고시회차 검색"""
        try:
            self.setup_driver()
            self.driver.get(self.base_url)
            logger.info("페이지 로딩 완료")
            
            # 마지막 페이지로 이동
            success = self.go_to_last_page()
            if not success:
                logger.warning("마지막 페이지로 이동 실패, 현재 페이지에서 계속 진행합니다.")
                
            # 현재 페이지에서 데이터 추출
            data = self.extract_exchange_data()
            if not data:
                logger.warning("데이터를 추출할 수 없습니다.")
                return None
            
            # 1. 정확히 session_number 찾기
            for item in data:
                if item["고시회차"] == session_number:
                    logger.info(f"'{session_number}'를 정확히 찾았습니다!")
                    return item
            
            # 2. 마지막 페이지 버튼을 한 번 더 클릭 시도
            logger.info(f"'{session_number}'를 찾지 못해 마지막 페이지 버튼을 한 번 더 클릭합니다")
            
            # 마지막 페이지 버튼이 없을 경우 인덱스를 줄여가며 시도
            found_button = False
            for index in range(11, 0, -1):  # 11부터 1까지 시도 (충분한 범위)
                try:
                    last_page_button_xpath = f"/html/body/div/div[4]/div/div[4]/div[3]/div[2]/div/div/a[{index}]"
                    last_page_button = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, last_page_button_xpath))
                    )
                    
                    # 버튼 텍스트 확인 (페이지 번호 또는 '>' 기호 등)
                    button_text = last_page_button.text.strip()
                    logger.info(f"발견한 버튼 (인덱스 {index}): '{button_text}'")
                    
                    # 버튼 클릭
                    self.driver.execute_script("arguments[0].click();", last_page_button)
                    logger.info(f"인덱스 {index}의 페이지 버튼 클릭 성공")
                    time.sleep(3)
                    found_button = True
                    break
                except Exception as e:
                    logger.debug(f"인덱스 {index}의 페이지 버튼 찾기 실패: {e}")
            
            if not found_button:
                logger.warning("마지막 페이지 버튼을 찾지 못했습니다.")
            
            # 페이지 다시 로드 후 데이터 추출
            data = self.extract_exchange_data()
            
            # 다시 session_number 찾기
            if data:
                for item in data:
                    if item["고시회차"] == session_number:
                        logger.info(f"마지막 페이지 추가 클릭 후 '{session_number}'를 찾았습니다!")
                        return item
            
            # 3. 숫자 추출 및 가장 낮은 숫자 찾기
            session_with_numbers = []
            if data:
                for item in data:
                    if "회" in item["고시회차"]:
                        numbers = re.findall(r'\d+', item["고시회차"])
                        if numbers:
                            try:
                                extracted_number = int(numbers[0])
                                session_with_numbers.append((extracted_number, item))
                                logger.info(f"숫자가 포함된 회차 발견: {item['고시회차']} (숫자: {extracted_number})")
                            except ValueError:
                                continue
            
                if session_with_numbers:
                    session_with_numbers.sort(key=lambda x: x[0])
                    lowest_session = session_with_numbers[0][1]
                    logger.info(f"가장 숫자가 낮은 회차 선택: {lowest_session['고시회차']} (숫자: {session_with_numbers[0][0]})")
                    return lowest_session
                
                # 4. 숫자가 있는 회차도 없는 경우, 첫 번째 항목 반환
                logger.info(f"숫자가 포함된 회차를 찾을 수 없어 첫 번째 항목 선택: {data[0]['고시회차']}")
                return data[0]
            
            logger.warning(f"'{session_number}'를 찾지 못했고, 대체할 항목도 없습니다.")
            return None
            
        except Exception as e:
            logger.error(f"검색 중 오류 발생: {e}")
            return None
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver 종료")
    
    def get_all_exchange_rates(self):
        """마지막 페이지의 모든 환율 정보 수집"""
        try:
            self.setup_driver()
            self.driver.get(self.base_url)
            logger.info("페이지 로딩 완료")
            
            # 마지막 페이지로 이동
            success = self.go_to_last_page()
            if not success:
                logger.warning("마지막 페이지로 이동 실패, 현재 페이지에서 계속 진행합니다.")
            
            # 현재 페이지에서 데이터 추출
            data = self.extract_exchange_data()
            
            if not data:
                logger.warning("수집된 데이터가 없습니다.")
                
            return data
                
        except Exception as e:
            logger.error(f"데이터 수집 중 오류 발생: {e}")
            return []
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver 종료")

def save_to_s3(data, bucket_name, file_name):
    """데이터를 S3에 저장"""
    try:
        if isinstance(data, list) and len(data) > 0:
            # 리스트 데이터를 CSV 형식의 문자열로 변환
            header = "고시회차,현재가 환율,수집일시\n"
            rows = []
            for item in data:
                row = f"{item['고시회차']},{item['현재가 환율']},{item['수집일시']}"
                rows.append(row)
            
            csv_content = header + "\n".join(rows)
            
            # S3에 업로드
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=csv_content,
                ContentType='text/csv',
                ContentEncoding='utf-8'
            )
            logger.info(f"데이터가 S3 버킷 '{bucket_name}'의 '{file_name}'에 저장되었습니다.")
            return True
        elif isinstance(data, dict):
            # 딕셔너리를 JSON 문자열로 변환
            json_data = json.dumps(data, ensure_ascii=False)
            
            # S3에 업로드
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=json_data,
                ContentType='application/json',
                ContentEncoding='utf-8'
            )
            logger.info(f"데이터가 S3 버킷 '{bucket_name}'의 '{file_name}'에 저장되었습니다.")
            return True
        else:
            logger.error("지원되지 않는 데이터 형식입니다.")
            return False
    except Exception as e:
        logger.error(f"S3 저장 중 오류 발생: {e}")
        return False

def handler(event, context):
    """Lambda 핸들러 함수"""
    try:
        # 버킷 이름 확인
        bucket_name = BUCKET_NAME
        if 'bucket_name' in event:
            bucket_name = event['bucket_name']
        
        # 찾을 회차 정보 (기본값: "1회")
        target_session = "1회"
        if 'target_session' in event:
            target_session = event['target_session']
        
        # 모든 데이터 수집 여부
        get_all = False
        if 'get_all' in event:
            get_all = event['get_all']
        
        # 크롤러 초기화
        crawler = DaumExchangeRateCrawler()
        
        # 결과 저장용 딕셔너리
        result_data = {
            "success": False,
            "message": "",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": None
        }
        
        # 모든 데이터 수집 또는 특정 회차 검색
        if get_all:
            logger.info("모든 환율 정보 수집 시작...")
            all_results = crawler.get_all_exchange_rates()
            
            if all_results and len(all_results) > 0:
                # 현재 날짜시간을 파일명에 포함
                file_name = f'exchange_rates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                
                # S3에 저장
                save_success = save_to_s3(all_results, bucket_name, file_name)
                
                if save_success:
                    result_data["success"] = True
                    result_data["message"] = f"총 {len(all_results)}개의 데이터 수집 완료 및 S3 저장 성공"
                    result_data["file_name"] = file_name
                else:
                    result_data["message"] = "데이터 수집은 완료되었으나 S3 저장 실패"
            else:
                result_data["message"] = "수집된 데이터가 없습니다."
        else:
            logger.info(f"'{target_session}' 검색 시작...")
            result = crawler.find_specific_session(target_session)
            
            if result:
                # 파일명 설정
                file_name = f'exchange_rate_{result["고시회차"].replace(" ", "_")}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
                
                # S3에 저장
                save_success = save_to_s3(result, bucket_name, file_name)
                
                if save_success:
                    result_data["success"] = True
                    result_data["message"] = f"'{result['고시회차']}' 데이터 수집 완료 및 S3 저장 성공"
                    result_data["data"] = result
                    result_data["file_name"] = file_name
                else:
                    result_data["message"] = "데이터 수집은 완료되었으나 S3 저장 실패"
                    result_data["data"] = result
            else:
                result_data["message"] = f"'{target_session}'를 찾지 못했습니다."
        
        return {
            'statusCode': 200,
            'body': json.dumps(result_data, ensure_ascii=False)
        }
        
    except Exception as e:
        logger.error(f"Lambda 실행 중 오류 발생: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': '크롤링 실행 중 오류가 발생했습니다.'
            }, ensure_ascii=False)
        }
if __name__ == '__main__':
    handler()
