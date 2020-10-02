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
    BeautifulSoup4 (bs4)
    PANDAS
    requests
    asyncio
    aiohttp
    nest_asyncio (OPTIONAL: Solves nested async calls in jupyter notebooks)
"""

import argparse
import time
from bs4 import BeautifulSoup
import pandas as pd
import random
import requests
import asyncio
import aiohttp
import socket
import warnings; warnings.filterwarnings('ignore') # aiohttp produces deprecation warnings that don't concern us
#import nest_asyncio; nest_asyncio.apply() # necessary to run nested async loops in jupyter notebooks

# Use a variety of agents for our ClientSession to reduce traffic per agent
# This (attempts to) avoid a ban for high traffic from any single agent
# We should really use proxybroker or similar to ensure no ban
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
    global articles_data
    async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
        async with semaphore, session.get(url) as response:
            data = await response.text()
            soup = BeautifulSoup(data, "lxml")
            # Get article abstract if exists - sometimes abstracts are not available (not an error)
            try:
                abstract_raw = soup.find('div', {'class': 'abstract-content selected'}).find_all('p')
                # Some articles are in a split background/objectives/method/results style, we need to join these paragraphs
                abstract = ' '.join([paragraph.text.strip() for paragraph in abstract_raw])
            except:
                abstract = 'NO_ABSTRACT'
            # Get author affiliations - sometimes affiliations are not available (not an error)
            affiliations = [] # list because it would be difficult to split since ',' exists within an affiliation
            try:
                all_affiliations = soup.find('ul', {'class':'item-list'}).find_all('li')
                for affiliation in all_affiliations:
                    affiliations.append(affiliation.get_text().strip())
            except:
                affiliations = 'NO_AFFILIATIONS'
            # Get article keywords - sometimes keywords are not available (not an error)
            try:
                # We need to check if the abstract section includes keywords or else we may get abstract text
                has_keywords = soup.find_all('strong',{'class':'sub-title'})[-1].text.strip()
                if has_keywords == 'Keywords:':
                    # Taking last element in following line because occasionally this section includes text from abstract
                    keywords = soup.find('div', {'class':'abstract' }).find_all('p')[-1].get_text()
                    keywords = keywords.replace('Keywords:','\n').strip() # Clean it up
                else:
                    keywords = 'NO_KEYWORDS'
            except:
                keywords = 'NO_KEYWORDS'
            try:
                title = soup.find('meta',{'name':'citation_title'})['content'].strip('[]')
            except:
                title = 'NO_TITLE'
            authors = ''    # string because it's easy to split a string on ','
            try:
                for author in soup.find('div',{'class':'authors-list'}).find_all('a',{'class':'full-name'}):
                    authors += author.text + ', '
                # alternative to get citation style authors (no first name e.g. I. Zenkov)
                # all_authors = soup.find('meta', {'name': 'citation_authors'})['content']
                # [authors.append(author) for author in all_authors.split(';')]
            except:
                authors = ('NO_AUTHOR')
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
            # Add dict containing one article's data to list of article dicts
            articles_data.append(article_data)

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
    # Return user specified number of pages if option was supplied
    if args.pages != None: return args.pages

    # Get search result page and wait a second for it to load
    # URL to the first page of results for a keyword search
    headers=make_header()
    search_url = f'{pubmed_url}+{keyword}'
    with requests.get(search_url,headers=headers) as response:
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
    # Set options so user can choose number of pages and publication date range to scrape, and output file name
    parser = argparse.ArgumentParser(description='Asynchronous PubMed Scraper')
    parser.add_argument('--pages', type=int, default=None, help='Specify number of pages to scrape for EACH keyword. Each page of PubMed results contains 10 articles. \n Default = all pages returned for all keywords.')
    parser.add_argument('--start', type=int, default=2019, help='Specify start year for publication date range to scrape. Default = 2019')
    parser.add_argument('--stop', type=int, default=2020, help='Specify stop year for publication date range to scrape. Default = 2020')
    parser.add_argument('--output', type=str, default='articles.csv',help='Choose output file name. Default = "articles.csv".')
    args = parser.parse_args()
    if args.output[-4:] != '.csv': args.output += '.csv' # ensure we save a CSV if user forgot to include format in --output option
    start = time.time()
    # This pubmed link is hardcoded to search for articles from user specified date range, defaults to 2019-2020
    pubmed_url = f'https://pubmed.ncbi.nlm.nih.gov/?term={args.start}%3A{args.stop}%5Bdp%5D'
    # The root pubmed link is used to construct URLs to scrape after PMIDs are retrieved from user specified date range
    root_pubmed_url = 'https://pubmed.ncbi.nlm.nih.gov'
    # Construct our list of keywords from a user input file to search for and extract articles from
    search_keywords = []
    with open('keywords.txt') as file:
        keywords = file.readlines()
        [search_keywords.append(keyword.strip()) for keyword in keywords]
    print(f'\nFinding PubMed article URLs for {len(keywords)} keywords found in keywords.txt\n')
    # Empty list to store all article data as List[dict]; each dict represents data from one article
    # This approach is considerably faster than appending article data article-by-article to a DataFrame
    articles_data = []
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
    print(f'Scraping initiated for {len(urls)} article URLs found from {args.start} to {args.stop}\n')
    # Get and run the loop to get article data into a DataFrame from a list of all URLs
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_article_data(urls))

    # Create DataFrame to store data from all articles
    articles_df = pd.DataFrame(articles_data, columns=['title','abstract','affiliations','authors','journal','date','keywords','url'])
    print('Preview of scraped article data:\n')
    print(articles_df.head(5))
    # Save all extracted article data to CSV for further processing
    filename = args.output
    articles_df.to_csv(filename)
    print(f'It took {time.time() - start} seconds to find {len(urls)} articles; {len(scraped_urls)} unique articles were saved to {filename}')

