import requests
from serpapi import GoogleSearch
from bs4 import BeautifulSoup
import re
import time
import json
import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os

# Cải tiến regex cho email: lấy cả dạng bị ngắt quãng bằng khoảng trắng/dấu chấm/khác, và có thể có ký tự lạ
EMAIL_REGEX = re.compile(
    r'([a-zA-Z0-9_.+-]+[\s\.\-]*@[a-zA-Z0-9-]+[\s\.\-]*\.[a-zA-Z0-9-.]+)',
    re.UNICODE
)

# Cải tiến regex cho số điện thoại: lấy các dạng có dấu chấm, dấu cách, dấu gạch ngang giữa các số
PHONE_REGEX = re.compile(
    r'(\b(?:0|\+84)[\s\-\.]?[1-9][0-9][\s\-\.]?([0-9]{3})[\s\-\.]?([0-9]{4,5})\b)',
    re.UNICODE
)

# Regex cho số điện thoại dạng 3 nhóm: 089.858.8618, 097.988.2246, 093 988 2246, 097-988-2246
PHONE_REGEX_GROUP = re.compile(
    r'(\b(?:0|\+84)[\s\-\.]?[1-9][0-9][\s\-\.]?[0-9]{3}[\s\-\.]?[0-9]{4,5}\b)',
    re.UNICODE
)

PRIORITY_KEYWORDS = [
    "contact", "about", "lien-he", "ho-tro", "support", "help", "hotline", "email", "phone", "lienhe", "lien_he",
    "call", "tel", "telephone", "address", "location", "map", "customer-service", "customer-care", "customer-support",
    "faq", "questions", "inquiry", "feedback", "message", "chat", "livechat", "contact-form", "contact-us", "reach-us",
    "get-in-touch", "connect", "info", "information", "mail", "mail-us", "email-support", "phone-number", "mobile",
    "zalo", "viber", "whatsapp", "messenger", "telegram", "wechat", "social", "social-media", "facebook", "twitter",
    "linkedin", "instagram", "youtube", "tiktok", "office", "head-office", "branch", "store-locator", "where-to-buy",
    "dealer", "distributor", "partner", "help-center", "helpdesk", "technical-support", "billing", "sales-contact",
    "dia-chi", "van-phong", "cua-hang", "he-thong", "chuong-trinh", "careers", "recruitment", "tuyen-dung", "tuyendung",
    "work-with-us", "collab", "collaborate", "doi-tac", "tuvan", "tu-van", "consult", "consultancy", "booking", "dat-lich",
    "schedule", "appointment", "tra-cuu", "tracking", "theo-doi", "complaint", "khiếu nại", "gop-y", "reviews", "danh-gia",
    "warranty", "bao-hanh", "return", "doi-tra", "refund", "hoan-tien", "shipping", "delivery", "van-chuyen", "payment",
    "thanh-toan", "pricing", "bang-gia", "quote", "bao-gia", "promotion", "khuyen-mai", "discount", "giam-gia", "event"
]

LOG_FILE = "crawl_log.jsonl"
RESULTS_FILE = "results.jsonl"
BLACKLIST_FILE = "blacklist_domains.json"
VISITED_FILE = "visited_domains.json"

# Sử dụng RLock cho phép 1 thread re-acquire lock nếu cần
log_lock = threading.RLock()
result_lock = threading.RLock()
blacklist_lock = threading.RLock()
visited_lock = threading.RLock()

def is_vietnam_domain(url): 
    return url

def normalize_email(raw_email):
    # Loại bỏ khoảng trắng, dấu . thừa giữa các phần email
    email = re.sub(r'[\s]+', '', raw_email)
    email = re.sub(r'\s*\.\s*', '.', email)
    email = re.sub(r'\s*\@\s*', '@', email)
    return email

def normalize_phone(raw_phone):
    # Loại bỏ khoảng trắng, dấu . thừa giữa các số
    phone = re.sub(r'[\s\.\-]+', '', raw_phone)
    return phone

def get_contacts(html):
    emails = set()
    phones = set()
    # Lấy email dạng bình thường và bị ngắt quãng
    for raw_email in EMAIL_REGEX.findall(html):
        email = normalize_email(raw_email)
        # Chỉ lấy email hợp lệ, kiểm tra lại
        if re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', email):
            emails.add(email)
    # Lấy số điện thoại dạng bình thường, dạng nhóm
    for raw_phone in PHONE_REGEX_GROUP.findall(html):
        phone = normalize_phone(raw_phone)
        if re.match(r'^(0|\+84)[1-9][0-9]{8,9}$', phone):
            phones.add(phone)
    # Thêm số điện thoại dạng số rời
    for raw_phone in PHONE_REGEX.findall(html):
        if isinstance(raw_phone, tuple):
            raw_phone = ''.join(raw_phone)
        phone = normalize_phone(raw_phone)
        if re.match(r'^(0|\+84)[1-9][0-9]{8,9}$', phone):
            phones.add(phone)
    return emails, phones

def is_priority_link(href, text):
    href_l = href.lower()
    text_l = text.lower() if text else ""
    for kw in PRIORITY_KEYWORDS:
        if kw in href_l or kw in text_l:
            return True
    return False

def get_links(html, base_url, domain_goc):
    soup = BeautifulSoup(html, 'html.parser')
    priority_links = []
    normal_links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        text = a_tag.text
        if href.startswith('http'):
            full_url = href
        elif href.startswith('/'):
            full_url = urljoin(base_url, href)
        else:
            continue
        if urlparse(full_url).netloc != domain_goc:
            continue
        if is_priority_link(full_url, text):
            priority_links.append(full_url)
        else:
            normal_links.append(full_url)
    return priority_links, normal_links

def get_proxy(proxies_list):
    if not proxies_list:
        return None
    proxy = proxies_list.pop(0)
    proxies_list.append(proxy)
    return {"http": proxy, "https": proxy}

def log_crawl(info):
    with log_lock:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            json.dump(info, f, ensure_ascii=False)
            f.write('\n')

def save_result(result):
    with result_lock:
        with open(RESULTS_FILE, 'a', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
            f.write('\n')

def load_json_set(filename):
    if not os.path.exists(filename):
        return set()
    with open(filename, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            return set(data)
        except Exception:
            return set()

def save_json_set(filename, data_set, lock_obj):
    with lock_obj:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(list(data_set), f, ensure_ascii=False)

def add_to_blacklist(domain, blacklist):
    with blacklist_lock:
        blacklist.add(domain)
        save_json_set(BLACKLIST_FILE, blacklist, blacklist_lock)

def add_to_visited(domain, visited_domains):
    with visited_lock:
        visited_domains.add(domain)
        save_json_set(VISITED_FILE, visited_domains, visited_lock)

def search_google_serpapi(query, serpapi_key, num=500):
    params = {
        "q": query,
        "gl": "us",
        "hl": "en",
        "num": num,
        "api_key": serpapi_key
    }
    search = GoogleSearch(params)
    results = search.get_dict()
    urls = []
    for result in results.get("organic_results", []):
        link = result.get("link")
        if link and is_vietnam_domain(link):
            urls.append(link)
    return urls

def crawl_one_site(
    url,
    max_depth=2,
    delay=1,
    proxies_list=None,
    blacklist=None,
    visited_domains=None,
    max_pages=20
):
    visited_urls = set()
    queue = [(url, 0)]
    domain_goc = urlparse(url).netloc

    min_delay = 1
    max_delay = 20
    crawled_pages = 0

    while queue:
        if crawled_pages >= max_pages:
            print(f"Đã đạt giới hạn {max_pages} bài trên domain {domain_goc}, chuyển sang site khác.")
            break

        url_crawl, depth = queue.pop(0)
        if url_crawl in visited_urls or depth > max_depth:
            continue

        visited_urls.add(url_crawl)
        crawled_pages += 1

        # Check blacklist
        with blacklist_lock:
            if domain_goc in blacklist:
                break

        print(f"Crawling: {url_crawl} (Depth: {depth})")
        proxy = get_proxy(proxies_list) if proxies_list else None
        crawl_start = datetime.datetime.utcnow().isoformat()
        log_info = {
            "url": url_crawl,
            "depth": depth,
            "crawl_start": crawl_start,
            "delay": delay,
        }
        try:
            response = requests.get(url_crawl, timeout=10, proxies=proxy)
            status_code = response.status_code
            html = response.text
            log_info["status_code"] = status_code
        except Exception as e:
            log_info["status_code"] = "EXCEPTION"
            log_info["error"] = str(e)
            log_info["emails_found"] = 0
            log_info["phones_found"] = 0
            log_info["skipped_reason"] = "request_exception"
            log_crawl(log_info)
            break

        # Kiểm soát tốc độ crawl tự động
        if status_code in [429, 503]:
            delay = min(delay * 2, max_delay)
            log_info["skipped_reason"] = f"Rate limit {status_code}, tăng delay lên {delay}s"
        else:
            delay = max(delay * 0.8, min_delay)

        emails, phones = get_contacts(html)
        log_info["emails_found"] = len(emails)
        log_info["phones_found"] = len(phones)
        log_info["skipped_reason"] = ""
        now = datetime.datetime.utcnow().isoformat()
        for email in emails:
            save_result({
                "type": "email",
                "value": email,
                "src_url": url_crawl,
                "crawl_time": now
            })
        for phone in phones:
            save_result({
                "type": "phone",
                "value": phone,
                "src_url": url_crawl,
                "crawl_time": now
            })
        log_crawl(log_info)

        # Nếu tìm thấy contact thì dừng crawl site này và thêm domain vào blacklist
        if emails or phones:
            add_to_blacklist(domain_goc, blacklist)
            break

        priority_links, normal_links = get_links(html, url_crawl, domain_goc)
        # Ưu tiên queue cho link ưu tiên lên trước
        for link in priority_links:
            if link not in visited_urls and len(queue) + crawled_pages < max_pages:
                queue.insert(0, (link, depth+1))
        for link in normal_links:
            if link not in visited_urls and len(queue) + crawled_pages < max_pages:
                queue.append((link, depth+1))
        time.sleep(delay)

    # Đánh dấu domain đã duyệt
    add_to_visited(domain_goc, visited_domains)

def run_crawler(
    keywords,
    serpapi_key,
    proxies_list=None,
    max_workers=20,
    max_depth=2,
    delay=2,
    max_pages_per_site=20
):
    # Load blacklist và visited từ file
    blacklist = load_json_set(BLACKLIST_FILE)
    visited_domains = load_json_set(VISITED_FILE)

    def crawl_task(site_url):
        domain = urlparse(site_url).netloc
        # Kiểm tra trong critical section để tránh race-condition
        with blacklist_lock, visited_lock:
            if (domain in blacklist) or (domain in visited_domains):
                print(f"SKIP (blacklist/visited): {site_url}")
                return
        try:
            crawl_one_site(
                site_url,
                max_depth=max_depth,
                delay=delay,
                proxies_list=proxies_list,
                blacklist=blacklist,
                visited_domains=visited_domains,
                max_pages=max_pages_per_site
            )
        except Exception as e:
            print(f"Error crawling {site_url}: {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for kw in keywords:
            print(f"Tìm kiếm từ khóa: {kw}")
            top_sites = search_google_serpapi(kw, serpapi_key, num=200)
            for site_url in top_sites:
                futures.append(executor.submit(crawl_task, site_url))
        for future in as_completed(futures):
            pass # Có thể thêm xử lý tiến trình, log...

if __name__ == "__main__":
    keywords = [
      "mortgage rates today",
        "health insurance plans",
        "credit score",
        "car insurance quotes",
        "best credit cards",
        "home renovation ideas",
        "student loans",
        "personal finance",
        "stock market",
        "real estate agent near me",
        "online courses",
        "web hosting",
        "digital marketing agency",
        "social media marketing",
        "fitness tracker",
        "healthy recipes",
        "weight loss tips",
        "mental health resources",
        "telehealth services",
        "pet insurance",
        "travel deals",
        "vacation packages",
        "flight tickets",
        "cheap hotels",
        "car rental",
        "home decor",
        "fashion trends",
        "sustainable products",
        "online shopping",
        "electronics store",
        "software development company",
        "cybersecurity services",
        "cloud computing",
        "data analytics",
        "mobile app development",
        "solar panel installation",
        "electric vehicles",
        "home security systems",
        "smart home devices",
        "financial advisor",
        "business loans",
        "small business ideas",
        "remote jobs",
        "career coaching",
        "resume writing service",
        "legal services",
        "tax preparation",
        "online banking",
        "investing for beginners",
        "life insurance"
       
    ]
    serpapi_key = "9010dd16ab4d6b39fea9a7d6c4f4c593c6fc18490851ea55b4b4b03162498d66"
    proxies_list = [
        # "http://proxy1:port",
        # "http://proxy2:port",
    ]
    run_crawler(
        keywords=keywords,
        serpapi_key=serpapi_key,
        proxies_list=proxies_list,
        max_workers=20, # Số luồng song song
        max_depth=2,
        delay=2,
        max_pages_per_site=20
    )
    print("Hoàn thành crawl, kết quả lưu ở results.jsonl (mỗi contact 1 dòng JSON)")
    print(f"Chi tiết log ở {LOG_FILE}")
    print(f"Danh sách blacklist: {BLACKLIST_FILE}")
    print(f"Danh sách đã duyệt: {VISITED_FILE}")


    # rm -rf .git

#     git remote set-url origin https://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@github.com/005019218299/crawl6.git
# git push -u origin main



# sudo apt update && sudo apt upgrade -y  
# sudo apt install python3 python3-pip python3-venv -y 
#python3 -m venv my_env
#source my_env/bin/activate
# pip install requests beautifulsoup4 serpapi google-search-results regex concurrent-log-handler urllib3 bs4
#pip install --upgrade pip
