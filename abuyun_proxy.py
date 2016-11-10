#coding=utf-8
import requests


def gen_abuyun_proxy():
    # proxy server
    ABUYUN_USER = "H778U07K14M4250P"
    ABUYUN_PASSWD = "FE04DDEF88A0CC9B"
    ABUYUN_HOST = "proxy.abuyun.com"
    ABUYUN_PORT = "9010"

    # authorization
    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host" : ABUYUN_HOST,
        "port" : ABUYUN_PORT,
        "user" : ABUYUN_USER,
        "pass" : ABUYUN_PASSWD,
    }
    proxies = {
        "http"  : proxyMeta,
        "https" : proxyMeta,
    }
    return proxies


def change_tunnel():
    """
    return 正在使用的 IP 地址;该 IP 已使用时长;该 IP 可继续使用时长
    """
    proxy_info = {}
    targetUrl = "http://proxy.abuyun.com/switch-ip"
    proxy = gen_abuyun_proxy()
    try:
        resp = requests.get(targetUrl, proxies=proxy)
        if resp.status_code == 200:
            ip, used_time, leaved_time = resp.text.strip().split(',')
            proxy_info = {'ip_addr': ip, 'used_time': int(used_time), 'leaved_time': int(leaved_time)}
    except Exception as e:
        print e
    return proxy_info


def test_abuyun():
    """
    Test
    """
    targetUrl = "http://test.abuyun.com/proxy.php"
    proxy = gen_abuyun_proxy()
    resp = requests.get(targetUrl, proxies=proxy)
    print resp.status_code
    print resp.text

def get_current_ip():
    """
    return 正在使用的 IP 地址;该 IP 已使用时长;该 IP 可继续使用时长
    """
    proxy_info = {}
    targetUrl = "http://proxy.abuyun.com/current-ip"
    proxy = gen_abuyun_proxy()
    try:
        resp = requests.get(targetUrl, proxies=proxy)
        if resp.status_code == 200:
            ip, used_time, leaved_time = resp.text.strip().split(',')
            proxy_info = {'ip_addr': ip, 'used_time': int(used_time), 'leaved_time': int(leaved_time)}
    except Exception as e:
        print e
    return proxy_info