# Asynchronous PubMed Scraper

## Quick Start
Instructions for Windows (assuming Linux users won't need this) <br>
0) Make sure you have [python](https://www.python.org/downloads/) installed <br>
1) Open command prompt and change directory to the folder containing async_pubmed_scraper.py and keywords.txt
2) Enter the following in command prompt to create a virtual environment: ```python -m pip install --user virtualenv```, ```python -m venv scraper_env```, ```.\env\Scripts\activate``` <br>
3) Enter ``` pip install -r requirements.txt``` in command prompt to install dependencies<br>
4) Enter your list of keywords to scrape, one per line, in 'keywords.txt' <br>
5) Enter python async_pubmed_scraper -h to get usage instructions, and get scraping!
Example: To scrape the first 10 pages of search results for your keywords from 2018 to 2020 and save the data to "article_data.csv", enter ```python async_pubmed_scraper --pages 10 --start 2018 --stop 2020 --output article_data``` <br>
## What it does 
This script asynchronously scrapes PubMed - an open-access database of scholarly research articles -
and saves the data to a PANDAS DataFrame which is then written to a CSV intended for further processing.
This script scrapes a user-specified list of keywords for all results pages asynchronously. <br>
### It gets an average speed of about 13 articles/second. 
Collects the following data: url, title, abstract, authors, affiliations, journal, keywords, date <br>

## Example Usage and Data

<img HTML='https://raw.githubusercontent.com/IliaZenkov/async-pubmed-scraper/master/example/cli_usage_example.JPG'/>
<img HTML='https://raw.githubusercontent.com/IliaZenkov/async-pubmed-scraper/master/example/data_example.JPG'/>

## Why scrape when there's an API? Why asynchronous?
PubMed provides an API - the NCBI Entrez API, also known as Entrez Programming Utilities or E-Utilities - 
which can be used build datasets with their search engine - however, PubMed allows only 3 URL requests per second 
through E-Utilities (10/second w/ API key).
We're doing potentially thousands of URL requests asynchronously - depending on the amount of articles returned by the command line options specified by the user. 
It's much faster to download articles from all urls on all results pages in parallel compared to downloading articles page by page, article by article, waiting for the previous article request to complete before moving on to a new one. It's not unusual to see a 10x speedup with async scraping compared to regular scraping.  
Simply put, we're going to make our client send requests to all search queries and all resulting article URLs at the same time.
Otherwise, our client will wait for the server to answer before sending the next request, so most of our script's execution time
will be spent waiting for a response from the (PubMed) server. 



