from collections import defaultdict
import csv
import datetime as dt
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATETIME_FORMAT = '%Y-%m-%d_%H-%M-%S'


class PepParsePipeline:
    def open_spider(self, spider):
        self.statuses = defaultdict(int)

    def process_item(self, item, spider):
        self.statuses[item['status']] += 1
        if not item['status']:
            raise KeyError
        return item

    def close_spider(self, spider):
        RESULTS_DIR = BASE_DIR / 'results'
        summary = [('Статус', 'Количество')]
        summary.extend(self.statuses.items())
        summary.append(('total', sum(self.statuses.values())))
        RESULTS_DIR.mkdir(exist_ok=True)
        now = dt.datetime.now().strftime(DATETIME_FORMAT)
        fname = f'status_summary_{now}.csv'
        fpath = RESULTS_DIR / fname
        with open(fpath, 'w', encoding='utf-8') as file:
            writer = csv.writer(file, dialect='unix')
            writer.writerows(summary)
