import asyncio
from asyncio import WindowsSelectorEventLoopPolicy
from pathlib import Path

asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
BOT_NAME = 'pep_parse'

SPIDER_MODULES = ['pep_parse.spiders']
NEWSPIDER_MODULE = 'pep_parse.spiders'

ROBOTSTXT_OBEY = True
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

FEEDS = {
    'results/pep_%(time)s.csv': {
        'format': 'csv',
        'fields': ['number', 'name', 'status'],
        'overwrite': True
    },
}

ITEM_PIPELINES = {
    'pep_parse.pipelines.PepParsePipeline': 300,
}

BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / 'results'
DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S'
