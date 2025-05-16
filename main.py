"""
宜昌市信用评价信息采集系统
版本: 3.0
功能说明:
1. 自动化采集企业信用评价数据
2. 支持验证码自动刷新机制
3. 数据解密采用AES-CBC模式 + PKCS7填充
4. 智能分页采集与数据校验
5. 多维度Excel报表生成
6. 自动生成分类排行榜JSON
7. 完善的错误处理与重试机制
8. 结构化日志记录
"""

import logging
import sys
import requests
import base64
import json
from Crypto.Cipher import AES
import time
from urllib.parse import quote
import random
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Union
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("credit_crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== 类型定义 ====================
class DecryptionError(Exception):
    """自定义解密异常"""
    def __init__(self, original_data: str = "", message: str = "AES解密失败"):
        self.original_data = original_data
        self.message = f"{message} | 原始数据: {original_data[:50]}"
        super().__init__(self.message)

# ==================== 配置常量 ====================
class Config:
    RETRY_COUNT = 3
    PAGE_RETRY_MAX = 2
    TIMEOUT = 15
    PAGE_SIZE = 10
    AES_KEY = b"6875616E6779696E6875616E6779696E"
    AES_IV = b"sskjKingFree5138"
    AES_BLOCK_SIZE = 16

    HEADERS = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,vi;q=0.7",
        "Connection": "keep-alive",
        "Content-Type": "application/json; charset=utf-8",
        "Host": "106.15.60.27:22222",
        "Referer": "http://106.15.60.27:22222/xxgs/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36"
    }

    COLUMN_SCHEMA = [
        {'id': 'cioName', 'name': '企业名称', 'width': 35, 'merge': True, 'align': 'left'},
        {'id': 'eqtName', 'name': '资质类别', 'width': 20, 'merge': True, 'align': 'center'},
        {'id': 'csf', 'name': '初始分', 'width': 12, 'merge': True, 'align': 'center', 'format': '0'},
        {'id': 'zzmx', 'name': '资质明细', 'width': 50, 'merge': False, 'align': 'left'},
        {'id': 'cxdj', 'name': '诚信等级', 'width': 12, 'merge': False, 'align': 'center'},
        {'id': 'score', 'name': '诚信分值', 'width': 12, 'merge': False, 'align': 'center', 'format': '0'},
        {'id': 'jcf', 'name': '基础分', 'width': 12, 'merge': False, 'align': 'center', 'format': '0'},
        {'id': 'zxjf', 'name': '专项加分', 'width': 12, 'merge': False, 'align': 'center', 'format': '0'},
        {'id': 'kf', 'name': '扣分', 'width': 12, 'merge': False, 'align': 'center', 'format': '0'},
        {'id': 'eqlId', 'name': '资质ID', 'width': 25, 'merge': False, 'align': 'center'},
        {'id': 'orgId', 'name': '组织ID', 'width': 30, 'merge': True, 'align': 'center'},
        {'id': 'cecId', 'name': '信用档案ID', 'width': 30, 'merge': True, 'align': 'center'}
    ]

# ==================== 核心功能模块 ====================
class CryptoUtils:
    @staticmethod
    def aes_decrypt(encrypted_base64: str) -> str:
        """
        AES-CBC解密 (PKCS7填充)
        
        :param encrypted_base64: Base64编码的加密字符串
        :return: 解密后的明文
        :raises DecryptionError: 解密失败时抛出
        """
        if not encrypted_base64:
            raise DecryptionError(message="加密数据为空")

        try:
            encrypted_bytes = base64.b64decode(encrypted_base64)
            if len(encrypted_bytes) % Config.AES_BLOCK_SIZE != 0:
                raise ValueError("密文长度不符合块大小要求")

            cipher = AES.new(Config.AES_KEY, AES.MODE_CBC, Config.AES_IV)
            decrypted_bytes = cipher.decrypt(encrypted_bytes)
            
            # PKCS7去除填充
            pad_length = decrypted_bytes[-1]
            if not (1 <= pad_length <= Config.AES_BLOCK_SIZE):
                raise ValueError("无效的填充长度")
            if decrypted_bytes[-pad_length:] != bytes([pad_length]) * pad_length:
                raise ValueError("填充验证失败")

            return decrypted_bytes[:-pad_length].decode("utf-8")
        except (ValueError, TypeError) as e:
            logger.error(f"解密参数错误: {str(e)}")
            raise DecryptionError(encrypted_base64[:50], f"参数错误: {str(e)}")
        except UnicodeDecodeError as e:
            logger.error(f"编码转换失败: {str(e)}")
            raise DecryptionError(encrypted_base64[:50], f"编码错误: {str(e)}")
        except Exception as e:
            logger.error(f"解密过程异常: {str(e)}")
            raise DecryptionError(encrypted_base64[:50], f"解密失败: {str(e)}")

class NetworkUtils:
    @staticmethod
    def safe_request(session: requests.Session, url: str) -> requests.Response:
        """
        安全请求方法，带自动重试机制
        
        :param session: requests会话对象
        :param url: 请求URL
        :return: 响应对象
        :raises RuntimeError: 超过最大重试次数时抛出
        """
        for attempt in range(Config.RETRY_COUNT):
            try:
                if attempt > 0:
                    delay = random.uniform(0.5, 2.5)
                    logger.debug(f"请求重试等待: {delay:.2f}s")
                    time.sleep(delay)

                logger.info(f"请求尝试 {attempt+1}/{Config.RETRY_COUNT} -> {url}")
                response = session.get(url, headers=Config.HEADERS, timeout=Config.TIMEOUT)
                response.raise_for_status()
                return response

            except requests.exceptions.Timeout as e:
                logger.warning(f"请求超时: {str(e)}")
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP错误: {e.response.status_code} {e.response.reason}")
            except requests.exceptions.RequestException as e:
                logger.error(f"请求异常: {str(e)}")

        logger.error(f"超过最大重试次数 {Config.RETRY_COUNT}")
        raise RuntimeError(f"请求失败，已尝试{Config.RETRY_COUNT}次")

class DataProcessor:
    @staticmethod
    def parse_response(encrypted_data: str) -> Dict:
        """
        响应数据解析方法
        
        :param encrypted_data: 加密的响应数据
        :return: 解析后的数据字典
        """
        if not encrypted_data:
            logger.warning("收到空响应数据")
            return {"error": "empty data"}

        try:
            decrypted_str = CryptoUtils.aes_decrypt(encrypted_data)
            logger.debug(f"解密数据样本: {decrypted_str[:100]}...")
            return json.loads(decrypted_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {str(e)}")
            return {"error": f"无效的JSON格式: {str(e)}"}
        except DecryptionError as e:
            logger.error(f"解密失败: {e.message}")
            return {"error": str(e)}

class ExcelExporter:
    @staticmethod
    def generate_report(data: List[Dict], github_mode: bool = False) -> Optional[Dict]:
        """
        生成Excel报告
        
        :param data: 数据集
        :param github_mode: GitHub模式标记
        :return: 生成文件路径信息
        """
        wb = Workbook()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # 数据预处理
            processed_data = ExcelExporter._process_raw_data(data)
            
            # 创建工作表
            sheets_config = [
                {"name": "企业信用数据汇总", "filter": lambda x: True},
                {"name": "建筑工程总承包", "filter": lambda x: "施工总承包_建筑工程_" in x.get("zzmx", "")},
                {"name": "市政公用工程", "filter": lambda x: "施工总承包_市政公用工程_" in x.get("zzmx", "")},
                {"name": "装修装饰工程", "filter": lambda x: "专业承包_建筑装修装饰工程_" in x.get("zzmx", "")}
            ]
            
            for config in sheets_config:
                sheet = wb.create_sheet(title=config["name"])
                filtered_data = filter(config["filter"], processed_data)
                ExcelExporter._fill_sheet(sheet, list(filtered_data))
            
            # 删除默认sheet
            if "Sheet" in wb.sheetnames:
                del wb["Sheet"]
            
            # 文件保存
            filename = ExcelExporter._get_output_filename(github_mode, timestamp)
            wb.save(filename)
            
            logger.info(f"报表生成成功: {filename}")
            return {"excel": filename, "json": []}
            
        except Exception as e:
            logger.error(f"报表生成失败: {str(e)}")
            return None

    @staticmethod
    def _process_raw_data(raw_data: List[Dict]) -> List[Dict]:
        """数据预处理"""
        processed = []
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            processed.extend(ExcelExporter._transform_item(item))
        return processed

    @staticmethod
    def _transform_item(item: Dict) -> List[Dict]:
        """数据项转换"""
        main_info = {
            'cioName': item.get('cioName', ''),
            'eqtName': item.get('eqtName', ''),
            'csf': int(float(item.get('csf', 0))),
            'orgId': item.get('orgId', ''),
            'cecId': item.get('cecId', ''),
            'zzmx': ''
        }
        
        details = item.get('zzmxcxfArray', [])
        if not details:
            return [main_info]
            
        return [{
            **main_info,
            'zzmx': detail.get('zzmx', ''),
            'cxdj': detail.get('cxdj', ''),
            'score': int(float(detail.get('score', 0))),
            'jcf': int(float(detail.get('jcf', 0))),
            'zxjf': int(float(detail.get('zxjf', 0))),
            'kf': int(float(detail.get('kf', 0))),
            'eqlId': detail.get('eqlId', '')
        } for detail in details]

    @staticmethod
    def _fill_sheet(sheet, data: List[Dict]):
        """填充工作表数据"""
        # 表头设置
        for col_idx, col in enumerate(Config.COLUMN_SCHEMA, 1):
            cell = sheet.cell(row=1, column=col_idx, value=col['name'])
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="003366")
            cell.alignment = Alignment(horizontal="center", vertical="center")
            sheet.column_dimensions[get_column_letter(col_idx)].width = col['width']
        
        # 数据行填充
        for row_idx, row_data in enumerate(data, 2):
            for col_idx, col in enumerate(Config.COLUMN_SCHEMA, 1):
                cell = sheet.cell(row=row_idx, column=col_idx, value=row_data.get(col['id'], ''))
                cell.alignment = Alignment(horizontal=col['align'], vertical="center")

    @staticmethod
    def _get_output_filename(github_mode: bool, timestamp: str) -> str:
        """生成输出文件名"""
        if github_mode:
            output_dir = os.path.join(os.getcwd(), "reports")
            os.makedirs(output_dir, exist_ok=True)
            return os.path.join(output_dir, f"信用评价_{timestamp}.xlsx")
        return f"宜昌市信用评价信息_{timestamp}.xlsx"

# ==================== 主程序 ====================
class CreditCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.current_code = ""
        self.current_ts = ""

    def run(self):
        """主执行流程"""
        logger.info("=== 爬虫启动 ===")
        try:
            self._refresh_captcha()
            total = self._get_total_pages()
            data = self._crawl_all_pages(total)
            self._export_data(data)
        except KeyboardInterrupt:
            logger.info("用户中断操作")
        except Exception as e:
            logger.error(f"爬虫异常终止: {str(e)}")
            raise
        finally:
            self.session.close()
            logger.info("=== 爬虫结束 ===")

    def _refresh_captcha(self):
        """刷新验证码"""
        url = f"http://106.15.60.27:22222/ycdc/bakCmisYcOrgan/getCreateCode?codeValue={int(time.time()*1000)}"
        response = NetworkUtils.safe_request(self.session, url).json()
        
        if response.get("code") != 0:
            raise RuntimeError(f"验证码接口异常: {response}")
            
        self.current_code = CryptoUtils.aes_decrypt(response["data"])
        self.current_ts = str(int(time.time() * 1000))
        logger.info(f"验证码刷新成功: {self.current_code[:4]}****")

    def _get_total_pages(self) -> int:
        """获取总页数"""
        page_data = self._fetch_page(1)
        total = page_data.get("total", 0)
        logger.info(f"总记录数: {total}")
        return (total + Config.PAGE_SIZE - 1) // Config.PAGE_SIZE

    def _fetch_page(self, page: int) -> Dict:
        """获取单页数据"""
        url = (
            "http://106.15.60.27:22222/ycdc/bakCmisYcOrgan/getCurrentIntegrityPage"
            f"?pageSize={Config.PAGE_SIZE}&cioName=%E5%85%AC%E5%8F%B8&page={page}"
            f"&code={quote(self.current_code)}&codeValue={self.current_ts}"
        )
        response = NetworkUtils.safe_request(self.session, url).json()
        return DataProcessor.parse_response(response.get("data", ""))

    def _crawl_all_pages(self, total_pages: int) -> List[Dict]:
        """分页爬取所有数据"""
        collected_data = []
        for page in range(1, total_pages + 1):
            retry = 0
            while retry < Config.PAGE_RETRY_MAX:
                try:
                    page_data = self._fetch_page(page)
                    if not page_data.get("data"):
                        raise ValueError("空数据集")
                        
                    collected_data.extend(page_data["data"])
                    logger.info(f"第 {page}/{total_pages} 页采集完成，累计 {len(collected_data)} 条")
                    break
                except Exception as e:
                    retry += 1
                    logger.warning(f"第 {page} 页采集失败: {str(e)}")
                    self._refresh_captcha()
            else:
                logger.error(f"跳过第 {page} 页，超过最大重试次数")
        return collected_data

    def _export_data(self, data: List[Dict]):
        """导出数据"""
        if not data:
            logger.warning("无有效数据，跳过导出")
            return
            
        result = ExcelExporter.generate_report(data, github_mode=True)
        if result and os.getenv('GITHUB_ACTIONS'):
            with open(os.environ['GITHUB_OUTPUT'], 'a') as fh:
                print(f"file-path={result['excel']}", file=fh)

if __name__ == "__main__":
    try:
        crawler = CreditCrawler()
        crawler.run()
    except Exception as e:
        logger.error(f"系统错误: {str(e)}")
        sys.exit(1)
