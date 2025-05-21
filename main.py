"""
宜昌市企业信用数据采集系统 (稳定版)
版本: 3.3
核心改进：
1. 增强型验证码处理机制
2. 多编码格式支持
3. 密钥动态验证系统
4. 智能诊断模式
5. 网络层深度优化
"""

import sys
import os
import json
import time
import random
import base64
import logging
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional, TypedDict, Any
from urllib.parse import quote
from datetime import datetime
from tqdm import tqdm

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openpyxl import Workbook
from Crypto.Cipher import AES

# ==================== 控制台界面 ====================
class ConsoleUI:
    """交互式控制台界面"""
    
    @staticmethod
    def show_header():
        print("\n" + "="*50)
        print("||  宜昌市企业信用数据采集系统  ||".center(46))
        print("||    (Version 3.3 稳定版)   ||".center(46))
        print("="*50)
        print(f"{'▶ 系统初始化中...':<40}", end='')

    @staticmethod
    def update_status(message: str, icon="🔄"):
        print(f"\r{icon} {message.ljust(50)}", end='')

    @staticmethod
    def show_footer(success: bool):
        result = "✅ 任务成功完成" if success else "❌ 任务异常终止"
        print("\n" + "="*50)
        print(result.center(50))
        print("="*50)

# ==================== 配置管理 ====================
@dataclass
class AppConfig:
    RETRY_COUNT: int = 3
    PAGE_SIZE: int = 20
    TIMEOUT: int = 20
    EXPORT_DIR: str = "reports"
    LOG_FILE: str = "logs/system.log"
    AES_KEY: bytes = os.getenv("AES_KEY", "6875616E6779696E6875616E6779696E").encode()
    AES_IV: bytes = os.getenv("AES_IV", "sskjKingFree5138").encode()

    @classmethod
    def setup(cls):
        os.makedirs(cls.EXPORT_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(cls.LOG_FILE), exist_ok=True)
        return cls()

# ==================== 核心模块 ====================
class NetworkEngine:
    """智能网络引擎"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.session = self._build_session()
        
    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.RETRY_COUNT,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        return session
    
    def safe_fetch(self, url: str) -> requests.Response:
        for attempt in range(1, self.config.RETRY_COUNT+1):
            try:
                ConsoleUI.update_status(f"请求 {url[:30]}...")
                response = self.session.get(url, timeout=self.config.TIMEOUT)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt == self.config.RETRY_COUNT:
                    raise NetworkError(f"请求失败: {str(e)}") from e
                time.sleep(2 ** attempt)

class DataHandler:
    """数据处理中心"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.cipher = AES.new(config.AES_KEY, AES.MODE_CBC, config.AES_IV)
        self._validate_cipher()

    def _validate_cipher(self):
        test_data = base64.b64decode("U2FsdGVkX19v4l0q9T/GbAsj6XQx1s2hLm4D7Jk=")
        decrypted = self.cipher.decrypt(test_data)
        if b"test" not in decrypted:
            raise RuntimeError("密钥验证失败")

    def decrypt_response(self, encrypted: str) -> Any:
        try:
            raw = base64.b64decode(encrypted)
            decrypted = self.cipher.decrypt(raw)
            return self._safe_decode(decrypted)
        except Exception as e:
            logging.error(f"解密失败数据: {encrypted[:100]}")
            raise

    def _safe_decode(self, data: bytes) -> Any:
        for encoding in ['utf-8', 'gb18030', 'latin-1']:
            try:
                return json.loads(data.decode(encoding).rstrip('\x00'))
            except UnicodeDecodeError:
                continue
        raise DecryptionError("无法解码数据")

class ReportBuilder:
    """报告生成器"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.columns = [
            ('企业名称', 'cioName', ''),
            ('资质类别', 'eqtName', ''),
            ('初始分', 'csf', 0),
            ('诚信分', 'score', 0)
        ]

    def create_report(self, data: List[Dict]) -> str:
        filename = f"{self.config.EXPORT_DIR}/report_{datetime.now():%Y%m%d%H%M}.xlsx"
        try:
            with Workbook(write_only=True) as wb:
                ws = wb.create_sheet("信用数据")
                ws.append([col[0] for col in self.columns])
                
                valid = 0
                for item in data:
                    if self._validate_item(item):
                        ws.append([item.get(col[1], col[2]) for col in self.columns])
                        valid +=1
                
                logging.info(f"有效数据率: {valid}/{len(data)}")
                wb.save(filename)
                return filename
        except Exception as e:
            if os.path.exists(filename):
                os.remove(filename)
            raise

    def _validate_item(self, item: Dict) -> bool:
        return all(key in item for key in ['cioName', 'score'])

# ==================== 主控制器 ====================
class CreditSystem:
    def __init__(self, config: AppConfig):
        self.config = config
        self.net = NetworkEngine(config)
        self.data = DataHandler(config)
        self.report = ReportBuilder(config)
        self.captcha = {'code': '', 'ts': ''}

    def execute(self) -> str:
        ConsoleUI.show_header()
        try:
            self._health_check()
            total = self._get_total()
            collected = self._collect_data(total)
            report_path = self.report.create_report(collected)
            ConsoleUI.show_footer(True)
            return report_path
        except Exception as e:
            ConsoleUI.show_footer(False)
            logging.error(traceback.format_exc())
            raise

    def _health_check(self):
        checks = [
            ("检查网络连接", self._check_network),
            ("获取验证码", self._get_captcha)
        ]
        for desc, task in checks:
            ConsoleUI.update_status(desc)
            task()

    def _check_network(self):
        test_url = "http://106.15.60.27:22222"
        if self.net.safe_fetch(test_url).status_code != 200:
            raise NetworkError("网络不可达")

    def _get_captcha(self):
        for _ in range(3):
            try:
                ts = str(int(time.time()*1000))
                resp = self.net.safe_fetch(
                    f"http://106.15.60.27:22222/ycdc/bakCmisYcOrgan/getCreateCode?codeValue={ts}"
                ).json()
                
                if not resp.get('data'):
                    continue
                
                self.captcha = {
                    'code': self.data.decrypt_response(resp['data']),
                    'ts': ts
                }
                return
            except Exception as e:
                logging.warning(f"验证码获取失败: {str(e)}")
        raise NetworkError("无法获取验证码")

    def _get_total(self) -> int:
        data = self._fetch_page(1)
        return (data['total'] + self.config.PAGE_SIZE - 1) // self.config.PAGE_SIZE

    def _fetch_page(self, page: int) -> Dict:
        url = (
            "http://106.15.60.27:22222/ycdc/bakCmisYcOrgan/getCurrentIntegrityPage"
            f"?pageSize={self.config.PAGE_SIZE}&page={page}"
            f"&code={quote(self.captcha['code'])}&codeValue={self.captcha['ts']}"
        )
        return self.data.decrypt_response(self.net.safe_fetch(url).json()['data'])

    def _collect_data(self, total_pages: int) -> List[Dict]:
        data = []
        with tqdm(total=total_pages, desc="采集进度") as bar:
            for page in range(1, total_pages+1):
                try:
                    page_data = self._fetch_page(page)
                    data.extend(page_data.get('data', []))
                    bar.update(1)
                except Exception as e:
                    logging.error(f"第{page}页错误: {str(e)}")
                    self._get_captcha()
        return data

# ==================== 执行入口 ====================
if __name__ == "__main__":
    try:
        config = AppConfig.setup()
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(config.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        
        system = CreditSystem(config)
        report = system.execute()
        print(f"\n生成报告位置: {os.path.abspath(report)}")
        sys.exit(0)
    except Exception as e:
        print(f"\n系统错误: {str(e)}")
        sys.exit(1)
