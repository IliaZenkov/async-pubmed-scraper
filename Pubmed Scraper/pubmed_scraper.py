import time
from bs4 import BeautifulSoup
import pandas as pd
import random
import re
from selenium import webdriver
import asyncio
import aiohttp
import socket
import nest_asyncio
nest_asyncio.apply()

"""
Author: Ilia Zenkov
Date: 9/26/2020

This script asynchronously scrapes Pubmed - an open-access database of scholarly research articles -
and saves the data to a DataFrame which is then written to a CSV for further processing in a separate script 

Contains the following functions:
    make_header:        Makes an HTTP request header using a random user agent
    get_num_pages:      Finds number of pubmed results pages returned by a keyword search
    extract_by_article: Extracts data from a single pubmed article to a DataFrame 
    get_pmids:          Gets PMIDs of all article URLs from a single page and builds URLs to pubmed articles specified by those PMIDs
    build_article_urls: Async wrapper for get_pmids, creates asyncio tasks for each page of results, page by page, 
                        and stores article urls in urls: List[string] 
    get_article_data:   Async wrapper for extract_by_article, creates asyncio tasks to scrape data from each article specified by urls[]

requires:
    for scraping:
        BeautifulSoup4 (bs4)
        PANDAS 
        requests
        selenium      
    for async:
        asyncio
        aiohttp
        nest_asyncio (to solve nested async calls in jupyter notebooks)      
"""

"""
Why scrape - why asynchronous?
PubMed provides an API - the NCBI Entrez API, also known as Entrez Programming Utilities or E-Utilities - 
which can be used build datasets with their search engine - however, PubMed allows only 3 queries per second 
through E-Utilities (10/second w/ API key).
Let's make a script to scrape PubMed. We're going to go for an asynchronous method,
as the speed up is too good to pass up. It will be much faster to download articles from
all urls on all results pages in parallel compared to downloading articles page by page, article by article. 
Simply put, we're going to make our client send requests to all search queries and all resulting article URLs at the same time.
Otherwise, our client will wait for the server to answer before sending the next request, so most of our script's execution time
will be spent waiting for a response from the (PubMed) server.  
See here for more info on asynchronous web scraping: (link)
We'll use nest_asyncio to avoid issues with event loop nesting in Jupyter notebooks (i.e nested asyncio calls)
Note also that asyncio + aiohttp is faster than the grequests library for async HTTP requests
"""

# Use a variety of agents for our ClientSession and webdriver to reduce traffic per agent
# This (attempts to) avoid a ban for high traffic from any single agent
# We should really use proxybroker to ensure no ban
user_agents = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
        ]

def make_header():
    '''
    Chooses a random agent from user_agents with which to construct headers
    :return: headers: dict: HTTP headers to use to get HTML from article URL
    '''
    # Make a header for the ClientSession/webdriver to use with one of our agents chosen at random
    headers = {
            'User-Agent':random.choice(user_agents),
            }
    return headers

async def extract_by_article(url):
    '''
    Extracts all data from a single article
    :param url: string: URL to a single article (i.e. root pubmed URL + PMID)
    :return: article_data: Dict: Contains all data from a single article
    '''
    conn = aiohttp.TCPConnector(family=socket.AF_INET)
    headers = make_header()

    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        async with semaphore, session.get(url) as response:
            data = await response.text()
            soup = BeautifulSoup(data, "lxml")

            # Get article abstract if exists - sometimes abstracts are not available (not an error)
            try:
                abstract = soup.find('div', {'class': 'abstract-content selected'}).find('p').get_text()
            except:
                abstract = 'NO_ABSTRACT'
            # Get author affiliations - sometimes affiliations are not available (not an error)
            affiliations = ''

            try:
                all_affiliations = soup.find('ul', {'class':'item-list'}).find_all('li')
                for affiliation in all_affiliations:
                    affiliations += ';' + affiliation.get_text()
            except:
                affiliations = 'NO_AFFILIATIONS'

            # Get article keywords - sometimes keywords are not available (not an error)
            try:
                keywords = soup.find('div', {'class':'abstract' }).find_all('p')[1].get_text().split(';')
            except:
                keywords = 'NO_KEYWORDS'

            #date_raw = soup.find('p', {'class': 'details'}).get_text()
            # Get the rest of the article data which should be always available
            title = soup.find('title').get_text()
            authors = []
            try:
                for author in soup.find('div', {'class': 'authors-list'}).find_all('a',{'class':'full-name'}):
                    authors.append(author.get_text())
            except: authors.append('')
            try:
                journal = soup.find('meta',{'name':'citation_journal_title'})['content']
            except: journal = ''
            date = 5
            #date_raw = soup.find('p', {'class': 'details'}).get_text()
            # Get time info with a regexp
            #date = re.findall(r'\d{4}[\s\w{3}\s\d+]*', date_raw)[0]

            # Format data as a dict to insert into a DataFrame
            article_data = {
                'url': url,
                'title': title,
                'authors': authors,
                'abstract': abstract,
                'affiliations': affiliations,
                'journal': journal,
                'keywords': keywords,
                'date': date
            }
            #all_articles[url] = title, authors, abstract, affiliations, journal, keywords, date
            print(article_data)

def get_num_pages(keyword):
    '''
    Gets total number of pages returned by search results for keyword
    :param keyword: string: search word used to search for results
    :return: num_pages: int: number of pages returned by search results for keyword
    '''
    # Get search result page and wait a second for it to load
    driver.get(search_url)
    driver.implicitly_wait(1)
    # Parse 1st page of keyword search results
    soup = BeautifulSoup(driver.page_source, "lxml")
    # Get total number of pages returned for the keyword
    num_pages = int((soup.find('span',{'class':'total-pages'}).get_text()).replace(',',''))
    return num_pages

async def get_pmids():
    """
    Extracts PMIDs of all articles from a pubmed search result, page by page,
    builds a url to each article, and stores all article URLs in urls: List[string]
    :return: None
    """
    headers = make_header()
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(search_url) as response:
            data = await response.text()
            # Parse the current page of search results from the response
            soup = BeautifulSoup(data, "lxml")
            # Find section which holds the PMIDs for all articles on a single page of search results
            page_content = soup.find_all('div', {'class': 'docsum-content'})
            # Extract URLs by getting PMIDs for all pubmed articles on the results page (default 10 articles/page)
            for line in page_content:
                url = pubmed_url + line.find('a').get('href')
                urls.append(url)

            # Click button on results page to navigate to the next page of results
            driver.find_element_by_xpath('//span[text()="Show more"]').click()

async def build_article_urls(keyword):
    """
    PubMed uniquely identifies articles using a PMID
    e.g. https://pubmed.ncbi.nlm.nih.gov/32023415/ #shameless self plug :)
    Any and all articles can be identified with a single PMID

    Async wrapper for get_article_urls, page by page of results, for a single search keyword
    :param keyword: string: search word used to search for results
    :return: None
    """
    tasks = []
    num_pages = get_num_pages(keyword)
    for page in range(num_pages):
        task = asyncio.create_task(get_pmids())
        tasks.append(task)

    await asyncio.gather(*tasks)

async def get_article_data(urls):
    """
    Async wrapper for extract_by_article to scrape data from each article (url)
    :param urls: List[string]: list of all pubmed urls returned by the search keyword
    :return: None
    """
    tasks = []
    for url in urls:
        task = asyncio.create_task(extract_by_article(url))
        tasks.append(task)

    await asyncio.gather(*tasks)

# The root pubmed link is hardcoded to search for articles only from the last 5 years (2015-2020)
pubmed_url = 'https://pubmed.ncbi.nlm.nih.gov/?term=2015%3A2020%5Bdp%5D'
search_keywords = ['wild dogs','wolves','wildlife','ecosystem population']
for keyword in search_keywords:
    # Construct a full pubmed URL for a keyword
    search_url = f'{pubmed_url}+{keyword}'
    all_articles = {}
    # Empty list to store all article URLs
    urls = []

    start = time.time()
    # We use a selenium webdriver to get total number of results pages returned by a search keyword
    #    and also to click to the next page of search results to continue getting PMIDs/article links
    driver = webdriver.Chrome('chromedriver')
    # We use asyncio's BoundedSemaphore method to limit the number of asynchronous requests
    #    we make to PubMed at a time to avoid a ban (and to be nice to PubMed servers)
    # Higher value for BoundedSemaphore yields faster scraping, and a higher chance of ban. 100-500 seems to be OK.
    semaphore = asyncio.BoundedSemaphore(100)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(build_article_urls(keyword))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_article_data(urls))

driver.quit()
print(all_articles)
#all_articles.to_csv(f'{search_keywords}.csv')
print(f'It took {time.time() - start} seconds to get {len(urls)} articles')

