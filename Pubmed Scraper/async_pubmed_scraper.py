"""
Author: Ilia Zenkov
Date: 9/26/2020

This script asynchronously scrapes Pubmed - an open-access database of scholarly research articles -
and saves the data to a DataFrame which is then written to a CSV intended for further processing
This script is capable of scraping a list of keywords asynchronously

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
        asyncio
        aiohttp
        nest_asyncio (to solve nested async calls in jupyter notebooks)
"""

"""
Why scrape - why asynchronous?
PubMed provides an API - the NCBI Entrez API, also known as Entrez Programming Utilities or E-Utilities - 
which can be used build datasets with their search engine - however, PubMed allows only 3 URL requests per second 
through E-Utilities (10/second w/ API key).
We're doing potentially thousands of URL requests asynchronously - depending on # semaphores, but usually hundreds per second. 
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

import time
from bs4 import BeautifulSoup
import pandas as pd
import random
import requests
import asyncio
import aiohttp
import socket
import nest_asyncio
nest_asyncio.apply()

# Use a variety of agents for our ClientSession to reduce traffic per agent
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
    :return headers: dict: HTTP headers to use to get HTML from article URL
    '''
    # Make a header for the ClientSession to use with one of our agents chosen at random
    headers = {
            'User-Agent':random.choice(user_agents),
            }
    return headers

async def extract_by_article(url):
    '''
    Extracts all data from a single article
    :param url: string: URL to a single article (i.e. root pubmed URL + PMID)
    :return article_data: Dict: Contains all data from a single article
    '''
    conn = aiohttp.TCPConnector(family=socket.AF_INET)
    headers = make_header()
    # Reference our articles DataFrame containing accumulated data for ALL scraped articles
    global articles
    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        async with semaphore, session.get(url) as response:
            data = await response.text()
            soup = BeautifulSoup(data, "lxml")
            # Get article abstract if exists - sometimes abstracts are not available (not an error)
            try:
                # Some articles are in a background/objectives/method/results style, we need to join these paragraphs
                abstract_raw = soup.find('div', {'class': 'abstract-content selected'}).find_all('p')
                abstract = ' '.join([paragraph.text.strip() for paragraph in abstract_raw])
            except:
                abstract = 'NO_ABSTRACT'
            # Get author affiliations - sometimes affiliations are not available (not an error)
            affiliations = []
            try:
                all_affiliations = soup.find('ul', {'class':'item-list'}).find_all('li')
                for affiliation in all_affiliations:
                    affiliations.append(affiliation.get_text())
            except:
                affiliations = 'NO_AFFILIATIONS'
            # Get article keywords - sometimes keywords are not available (not an error)
            try:
                keywords = soup.find('div', {'class':'abstract' }).find_all('p')[1].get_text().strip().split(';')
            except:
                keywords = 'NO_KEYWORDS'
            try:
                title = soup.find('meta',{'name':'citation_title'})['content']
            except:
                title = 'NO_TITLE'
            authors = []
            try:
                for author in soup.find('div',{'class':'authors-list'}).find_all('a',{'class':'full-name'}):
                    authors.append(author.text)
                # alternative to get citation style authors (e.g. I. Zenkov)
                # all_authors = soup.find('meta', {'name': 'citation_authors'})['content']
                # [authors.append(author) for author in all_authors.split(';')]
            except:
                authors.append('NO_AUTHOR')
            try:
                journal = soup.find('meta',{'name':'citation_journal_title'})['content']
            except:
                journal = 'NO_JOURNAL'
            try:
                date = soup.find('time', {'class': 'citation-year'}).text
            except:
                date = 'NO_DATE'

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
            # Add an individual article's data to the DataFrame containing all articles
            articles = articles.append(article_data, ignore_index=True)

async def get_pmids(page, keyword):
    """
    Extracts PMIDs of all articles from a pubmed search result, page by page,
    builds a url to each article, and stores all article URLs in urls: List[string]
    :param page: int: value of current page of a search result for keyword
    :param keyword: string: current search keyword
    :return: None
    """
    # URL to one unique page of results for a keyword search
    page_url = f'{pubmed_url}+{keyword}+&page={page}'
    headers = make_header()
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(page_url) as response:
            data = await response.text()
            # Parse the current page of search results from the response
            soup = BeautifulSoup(data, "lxml")
            # Find section which holds the PMIDs for all articles on a single page of search results
            pmids = soup.find('meta',{'name':'log_displayeduids'})['content']
            # alternative to get pmids: page_content = soup.find_all('div', {'class': 'docsum-content'}) + for line in page_content: line.find('a').get('href')
            # Extract URLs by getting PMIDs for all pubmed articles on the results page (default 10 articles/page)
            for pmid in pmids.split(','):
                url = root_pubmed_url + '/' + pmid
                urls.append(url)

def get_num_pages(keyword):
    '''
    Gets total number of pages returned by search results for keyword
    :param keyword: string: search word used to search for results
    :return: num_pages: int: number of pages returned by search results for keyword
    '''
    # Get search result page and wait a second for it to load
    conn = aiohttp.TCPConnector(family=socket.AF_INET)
    headers = make_header()
    # URL to the first page of results for a keyword search
    search_url = f'{pubmed_url}+{keyword}'
    with requests.get(search_url) as response:
        data = response.text
        soup = BeautifulSoup(data, "lxml")
        num_pages = int((soup.find('span', {'class': 'total-pages'}).get_text()).replace(',',''))
        return num_pages # Can hardcode this value (e.g. 10 pages) to limit # of articles scraped per keyword

async def build_article_urls(keywords):
    """
    PubMed uniquely identifies articles using a PMID
    e.g. https://pubmed.ncbi.nlm.nih.gov/32023415/ #shameless self plug :)
    Any and all articles can be identified with a single PMID

    Async wrapper for get_article_urls, page by page of results, for a single search keyword
    Creates an asyncio task for each page of search result for each keyword
    :param keyword: string: search word used to search for results
    :return: None
    """
    tasks = []
    for keyword in keywords:
        num_pages = get_num_pages(keyword)
        for page in range(1,num_pages+1):
            task = asyncio.create_task(get_pmids(page, keyword))
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
        if url not in scraped_urls:
            task = asyncio.create_task(extract_by_article(url))
            tasks.append(task)
            scraped_urls.append(url)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    start = time.time()

    # This pubmed link is hardcoded to search for articles only from the last 5 years (2015-2020)
    pubmed_url = 'https://pubmed.ncbi.nlm.nih.gov/?term=2015%3A2020%5Bdp%5D'
    # The root pubmed link is used to construct URLs to scrape after PMIDs are retrieved from 2015-2020
    root_pubmed_url = 'https://pubmed.ncbi.nlm.nih.gov'
    # Construct our list of keywords to search for and extract articles from
    search_keywords = ['wild dogs','wolves','wildlife','herp','derp']
    # Construct a full pubmed URL for a keyword
    articles = pd.DataFrame(columns=['title','abstract','affiliations','authors','journal','date','keywords','url'])
    # Empty list to store all article URLs
    urls = []
    # Empty list to store URLs already scraped
    scraped_urls = []

    # We use asyncio's BoundedSemaphore method to limit the number of asynchronous requests
    #    we make to PubMed at a time to avoid a ban (and to be nice to PubMed servers)
    # Higher value for BoundedSemaphore yields faster scraping, and a higher chance of ban. 100-500 seems to be OK.
    semaphore = asyncio.BoundedSemaphore(100)

    # Get and run the loop to build a list of all URLs
    loop = asyncio.get_event_loop()
    loop.run_until_complete(build_article_urls(search_keywords))
    # Get and run the loop to get article data into a DataFrame from a list of all URLs
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_article_data(urls))

    # Save all extracted article data to CSV for further processing
    print(articles)
    articles.to_csv('articles.csv')
    print(f'It took {time.time() - start} seconds to find {len(urls)} articles and scrape {len(scraped_urls)} unique articles')

