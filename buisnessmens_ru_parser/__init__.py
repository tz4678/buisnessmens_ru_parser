import argparse
import logging
import math
import os
import queue
from pathlib import Path
from threading import Event, Thread
from typing import Dict, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

__version__ = '0.1.0'

BASE_URL = 'https://businessmens.ru/'

log = logging.getLogger(__name__.split('.')[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('username', help='Username')
    parser.add_argument('password', help='Password')
    parser.add_argument(
        '-o', '--output', help='Output filename', default='emails.txt'
    )
    parser.add_argument('-t', '--topic', help='Topic', default='all')
    parser.add_argument(
        '-w',
        '--num_workers',
        help='Number of workers',
        default=os.cpu_count(),
        type=int,
    )
    parser.add_argument('--timeout', help='Timeout', default=15.0, type=float)
    parser.add_argument(
        '--user-agent',
        help='User Agent',
        default=(
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            ' (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36'
            ' Edge/12.246'
        ),
    )
    args = parser.parse_args()
    logging.basicConfig()
    log.setLevel(logging.DEBUG)
    headers = {'User-Agent': args.user_agent}
    session = requests.Session()
    r = session.get(
        urljoin(BASE_URL, '/login'), headers=headers, timeout=args.timeout
    )
    s = BeautifulSoup(r.text, 'lxml')
    el = s.find('input', {'name': '_csrf'}, type='hidden')
    assert el
    csrf = el.attrs['value']
    r = session.post(
        urljoin(BASE_URL, '/login'),
        headers=headers,
        data={
            '_csrf': csrf,
            'Login[username]': args.username,
            'Login[password]': args.password,
        },
        timeout=args.timeout,
    )
    if r.url.endswith('/login'):
        raise Exception('invalid username or password')
    assert r.status_code == 200
    assert r.url == BASE_URL
    page = 1
    pages = 1
    q = queue.Queue()
    stopped = Event()
    emails = set()
    workers = []
    for _ in range(args.num_workers):
        t = Thread(
            target=worker,
            kwargs=dict(
                q=q,
                stopped=stopped,
                emails=emails,
                headers=headers,
                cookies=session.cookies,
                timeout=args.timeout,
            ),
        )
        t.daemon = True
        workers.append(t)
        t.start()
    while page <= pages:
        try:
            r = session.get(
                urljoin(BASE_URL, f'/franchise/{args.topic}/{page}'),
                headers=headers,
                timeout=args.timeout,
            )
            log.debug(r.url)
            s = BeautifulSoup(r.text, 'lxml')
            el = s.find('p', class_='franchise-category__list-count')
            assert el
            # Показано франшиз: 10 из 1652
            per_page, total = map(
                int, el.string.strip().split(': ')[1].split(' из ')
            )
            pages = math.floor(total / per_page)
            for link in s.find_all('a', class_='fr-item__link-name'):
                q.put_nowait(link.attrs['href'])
        except Exception as ex:
            log.exception(ex)
        page += 1
    q.join()
    stopped.set()
    for i in range(args.num_workers):
        workers[i].join()
    with Path(args.output).expanduser().open('w+') as fp:
        fp.write('\n'.join(emails))
    log.info('finished')


def worker(
    *,
    q: queue.Queue,
    stopped: Event,
    emails: Set[str],
    headers: Dict[str, str],
    cookies: 'requests.cookies.RequestCookieJar',
    timeout: float,
) -> None:
    while not stopped.is_set():
        try:
            url = q.get(timeout=10)
        except queue.Empty:
            continue
        try:
            r = requests.get(
                urljoin(BASE_URL, url), cookies=cookies, timeout=timeout
            )
            s = BeautifulSoup(r.text, 'lxml')
            link = s.find('a', class_='website linkForReg need-auth')
            assert link
            url = urljoin(BASE_URL, link.attrs['href'])
            r = requests.get(url, cookies=cookies, timeout=timeout)
            log.debug('redirect => %s', r.url)
            s = BeautifulSoup(r.text, 'lxml')
            for el in s.select('a[href^="mailto:"]'):
                email = el.attrs['href'][7:]
                emails.add(email)
        except Exception as ex:
            log.exception(ex)
        finally:
            q.task_done()
