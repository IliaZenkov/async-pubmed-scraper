# async-pubmed-scraper
## What it does 
This script asynchronously scrapes PubMed - an open-access database of scholarly research articles -
and saves the data to a DataFrame which is then written to a CSV intended for further processing
This script scrapes a user-specified list of keywords and their results pages asynchronously.
Collects the following data: url, title, abstract, authors, affiliations, journal, keywords, date

## Why scrape when there's an API? Why asynchronous?
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
