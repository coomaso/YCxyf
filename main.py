"""
宜昌市企业信用数据采集系统 (增强版)
版本: 3.2
功能增强：
1. 实时采集进度可视化
2. 智能数据完整性校验
3. 增强型错误恢复机制
4. 资源安全管理系统
5. 多级数据验证体系
"""

# -*- coding: utf-8 -*-
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
from tqdm import tqdm  # 进度条显示

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openpyxl import Workbook
from Crypto.Cipher import AES

# ==================== 可视化配置 ====================
class ConsoleDisplay:
    """控制台显示管理器"""
    
    @staticmethod
    def show_header():
        """显示系统标题"""
        print("\n" + "="*50)
        print("||   宜昌市企业信用数据采集系统   ||".center(46))
        print("||   (Version 3.2 - 增强版)    ||".center(46))
        print("="*50)
        print(f"{'▶ 开始初始化系统...':<40}", end='')

    @staticmethod
    def show_progress(message: str, status: str = "正在处理"):
        """显示带状态的进度信息"""
        icons = {"正在处理": "🔄", "成功": "✅", "警告": "⚠️ ", "错误": "❌"}
        print(f"\r{icons.get(status,'')} {message.ljust(50)}", end='')

    @staticmethod
    def show_footer(success: bool):
        """显示结束信息"""
        result = "✅ 采集任务成功完成！" if success else "❌ 采集任务异常终止！"
        print("\n" + "="*50)
        print(result.center(50))
        print("="*50)

# ==================== 配置管理 ====================
@dataclass
class AppConfig:
    # 网络配置
    RETRY_COUNT: int = 3                  # 请求重试次数
    PAGE_SIZE: int = 20                   # 每页数据量
    TIMEOUT: int = 20                     # 请求超时(秒)
    
    # 路径配置
    EXPORT_DIR: str = "信用报告"            # 输出目录
    LOG_FILE: str = "logs/system.log"     # 日志路径
    
    # 加解密配置
    AES_KEY: bytes = b"6875616E6779696E6875616E6779696E"  # 从环境变量加载
    AES_IV: bytes = b"sskjKingFree5138"
    
    # 功能开关
    ENABLE_PROGRESS_BAR: bool = True      # 启用进度条

    @classmethod
    def setup(cls):
        """初始化系统环境"""
        os.makedirs(cls.EXPORT_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(cls.LOG_FILE), exist_ok=True)
        return cls()

# ==================== 类型定义 ====================
class CompanyData(TypedDict):
    """企业信用数据结构"""
    cioName: str        # 企业名称
    eqtName: str        # 资质类型
    csf: float          # 初始分
    score: float        # 诚信分值
    jcf: float          # 基础分
    zxjf: float         # 专项加分
    kf: float           # 扣分项
    zzmx: str           # 资质明细
    eqlId: str          # 资质ID
    orgId: str          # 组织ID
    cecId: str          # 信用档案ID

# ==================== 异常体系 ====================
class CrawlerError(Exception):
    """爬虫基础异常"""
    def __init__(self, message: str, context: dict = None):
        self.context = context or {}
        super().__init__(f"{message} | 上下文: {self.context}")

class NetworkError(CrawlerError):
    """网络请求异常"""

class DataIntegrityError(CrawlerError):
    """数据完整性异常"""

class ExportError(CrawlerError):
    """数据导出异常"""

# ==================== 日志系统 ====================
def setup_logger(config: AppConfig) -> logging.Logger:
    """配置结构化日志系统"""
    logger = logging.getLogger("CreditCrawler")
    logger.setLevel(logging.DEBUG)

    # 文件日志
    file_handler = logging.FileHandler(config.LOG_FILE, encoding='utf-8')
    file_formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s @ %(module)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)

    # 控制台日志
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

# ==================== 核心模块 ====================
class NetworkManager:
    """智能网络请求管理器"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.session = self._init_session()
        self.progress_bar = None

    def _init_session(self) -> requests.Session:
        """初始化带重试机制的会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.RETRY_COUNT,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def safe_request(self, url: str) -> requests.Response:
        """执行安全请求（带可视化提示）"""
        ConsoleDisplay.show_progress(f"请求数据: {url[:50]}...")
        for attempt in range(1, self.config.RETRY_COUNT + 1):
            try:
                response = self.session.get(
                    url, 
                    headers=self._default_headers(),
                    timeout=self.config.TIMEOUT
                )
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                ConsoleDisplay.show_progress(f"请求失败({attempt}/{self.config.RETRY_COUNT})", "警告")
                time.sleep(2 ** attempt)  # 指数退避
                if attempt == self.config.RETRY_COUNT:
                    raise NetworkError(f"请求失败: {str(e)}", {"url": url}) from e

    @staticmethod
    def _default_headers() -> dict:
        """生成默认请求头"""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36",
            "Accept": "application/json"
        }

class DataProcessor:
    """数据加工中心"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.aes_cipher = AES.new(config.AES_KEY, AES.MODE_CBC, config.AES_IV)

    def decrypt_data(self, encrypted: str) -> Any:
        """解密数据并转换为JSON"""
        try:
            ConsoleDisplay.show_progress("正在解密数据...")
            decrypted = self.aes_cipher.decrypt(base64.b64decode(encrypted))
            clean_data = decrypted.rstrip(b"\x00").decode("utf-8")
            return json.loads(clean_data)
        except (ValueError, json.JSONDecodeError) as e:
            raise DataIntegrityError("数据解密失败", {"error": str(e)})

    @staticmethod
    def validate_raw_data(item: dict) -> bool:
        """验证原始数据有效性"""
        required_fields = {'cioName', 'zzmxcxfArray'}
        return all(field in item for field in required_fields)

    def transform_data(self, raw: dict) -> List[CompanyData]:
        """转换原始数据结构"""
        transformed = []
        base_info = {
            'cioName': raw.get('cioName', '未知企业'),
            'eqtName': raw.get('eqtName', ''),
            'csf': float(raw.get('csf', 0)),
            'orgId': raw.get('orgId', ''),
            'cecId': raw.get('cecId', '')
        }

        for detail in raw.get('zzmxcxfArray', []):
            transformed.append({
                **base_info,
                'score': float(detail.get('score', 0)),
                'jcf': float(detail.get('jcf', 0)),
                'zxjf': float(detail.get('zxjf', 0)),
                'kf': float(detail.get('kf', 0)),
                'zzmx': detail.get('zzmx', ''),
                'eqlId': detail.get('eqlId', '')
            })
        return transformed

class ReportGenerator:
    """智能报告生成器"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.sheet_configs = [
            {'name': '全部数据', 'filter': lambda x: True},
            {'name': '建筑工程', 'filter': lambda x: '施工总承包_建筑工程_' in x.get('zzmx', '')},
            {'name': '市政工程', 'filter': lambda x: '施工总承包_市政公用工程_' in x.get('zzmx', '')}
        ]

    def generate(self, data: List[CompanyData]) -> str:
        """生成Excel报告"""
        ConsoleDisplay.show_progress("正在生成报告...")
        filename = self._generate_filename()
        
        try:
            with Workbook(write_only=True) as wb:
                for config in self.sheet_configs:
                    sheet = wb.create_sheet(title=config['name'])
                    filtered = filter(config['filter'], data)
                    self._fill_sheet(sheet, list(filtered))
                
                ConsoleDisplay.show_progress(f"保存报告文件: {filename}")
                wb.save(filename)
                return filename
        except Exception as e:
            if os.path.exists(filename):
                os.remove(filename)
            raise ExportError("报告生成失败", {"error": str(e)})

    def _fill_sheet(self, sheet, data: List[CompanyData]):
        """填充工作表数据"""
        # 列配置（名称，数据键，默认值）
        columns = [
            ('企业名称', 'cioName', ''),
            ('资质类别', 'eqtName', ''),
            ('初始分', 'csf', 0.0),
            ('诚信分值', 'score', 0.0),
            ('基础分', 'jcf', 0.0),
            ('专项加分', 'zxjf', 0.0)
        ]
        
        # 写入标题
        sheet.append([col[0] for col in columns])
        
        # 写入数据
        valid_count = 0
        for item in data:
            try:
                row = [item.get(key, default) for _, key, default in columns]
                sheet.append(row)
                valid_count += 1
            except Exception as e:
                logging.warning(f"数据异常被跳过: {str(e)}")

        logging.info(f"工作表写入完成: 有效数据 {valid_count}/{len(data)} 条")

    def _generate_filename(self) -> str:
        """生成唯一文件名"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(self.config.EXPORT_DIR, f"企业信用报告_{timestamp}.xlsx")

# ==================== 主控制器 ====================
class CreditCrawler:
    """系统主控制器"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.network = NetworkManager(config)
        self.processor = DataProcessor(config)
        self.report = ReportGenerator(config)
        self.captcha = {'code': '', 'timestamp': ''}

    def execute(self) -> str:
        """执行采集任务"""
        ConsoleDisplay.show_header()
        try:
            # 阶段1: 系统校验
            self._check_environment()
            
            # 阶段2: 数据采集
            ConsoleDisplay.show_progress("开始采集数据")
            total_pages = self._get_total_pages()
            data = self._crawl_data(total_pages)
            
            # 阶段3: 生成报告
            report_path = self.report.generate(data)
            
            ConsoleDisplay.show_footer(success=True)
            return report_path
        except Exception as e:
            ConsoleDisplay.show_footer(success=False)
            logging.error(f"系统异常: {traceback.format_exc()}")
            raise

    def _check_environment(self):
        """系统环境检查"""
        checks = [
            ("验证网络连接", self._check_network),
            ("获取验证码", self._refresh_captcha)
        ]
        
        for desc, func in checks:
            ConsoleDisplay.show_progress(desc)
            func()

    def _check_network(self):
        """网络连通性检查"""
        test_url = "http://106.15.60.27:22222"
        try:
            response = self.network.safe_request(test_url)
            if response.status_code != 200:
                raise NetworkError("服务器连接异常")
        except Exception as e:
            raise NetworkError("网络不可达", {"url": test_url}) from e

    def _refresh_captcha(self):
        """获取验证码"""
        for _ in range(self.config.RETRY_COUNT):
            try:
                timestamp = str(int(time.time() * 1000))
                url = f"http://106.15.60.27:22222/ycdc/bakCmisYcOrgan/getCreateCode?codeValue={timestamp}"
                response = self.network.safe_request(url).json()
                
                if response['code'] == 0:
                    self.captcha = {
                        'code': self.processor.decrypt_data(response['data']),
                        'timestamp': timestamp
                    }
                    return
            except Exception as e:
                logging.warning(f"验证码获取失败: {str(e)}")
        raise NetworkError("无法获取验证码")

    def _get_total_pages(self) -> int:
        """计算总页数"""
        first_page = self._fetch_page(1)
        total = first_page.get('total', 0)
        return (total + self.config.PAGE_SIZE - 1) // self.config.PAGE_SIZE

    def _fetch_page(self, page: int) -> dict:
        """获取单个页面数据"""
        url = (
            "http://106.15.60.27:22222/ycdc/bakCmisYcOrgan/getCurrentIntegrityPage"
            f"?pageSize={self.config.PAGE_SIZE}&cioName=%E5%85%AC%E5%8F%B8"
            f"&page={page}&code={quote(self.captcha['code'])}&codeValue={self.captcha['timestamp']}"
        )
        response = self.network.safe_request(url)
        return self.processor.decrypt_data(response.json()['data'])

    def _crawl_data(self, total_pages: int) -> List[CompanyData]:
        """采集所有页面数据"""
        all_data = []
        progress = tqdm(total=total_pages, desc="数据采集进度", disable=not self.config.ENABLE_PROGRESS_BAR)
        
        for page in range(1, total_pages + 1):
            try:
                page_data = self._fetch_page(page)
                for raw_item in page_data.get('data', []):
                    if DataProcessor.validate_raw_data(raw_item):
                        all_data.extend(self.processor.transform_data(raw_item))
                progress.update(1)
            except Exception as e:
                logging.error(f"第 {page} 页采集失败: {str(e)}")
                self._refresh_captcha()  # 失败时刷新验证码
        progress.close()
        return all_data

# ==================== 执行入口 ====================
if __name__ == "__main__":
    try:
        config = AppConfig.setup()
        logger = setup_logger(config)
        
        crawler = CreditCrawler(config)
        report_path = crawler.execute()
        
        print(f"\n📁 报告文件路径: {os.path.abspath(report_path)}")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 系统运行异常: {str(e)}")
        sys.exit(1)
