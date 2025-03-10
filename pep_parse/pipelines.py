from collections import defaultdict
import csv
from datetime import datetime as dt
from pathlib import Path
from scrapy.exporters import CsvItemExporter

BASE_DIR = Path(__file__).resolve().parent.parent
DATETIME_FORMAT = '%Y-%m-%dT%H-%M-%S'


class PepParsePipeline:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        RESULTS_DIR = BASE_DIR / 'results'
        RESULTS_DIR.mkdir(exist_ok=True)
        timestamp = dt.now().strftime(DATETIME_FORMAT)
        self.statuses = defaultdict(int)
        self.file = open(RESULTS_DIR / f'pep_{timestamp}.csv', 'wb')
        self.exporter = CsvItemExporter(
            self.file, fields_to_export=['number', 'name', 'status']
        )

    def open_spider(self, spider):
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.statuses[item['status']] += 1
        if not item.get('status'):
            raise KeyError
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        RESULTS_DIR = BASE_DIR / 'results'
        summary = [('Статус', 'Количество')]
        summary.extend(self.statuses.items())
        summary.append(('total', sum(self.statuses.values())))
        RESULTS_DIR.mkdir(exist_ok=True)
        now = dt.now().strftime(DATETIME_FORMAT)
        fname = f'status_summary_{now}.csv'
        fpath = RESULTS_DIR / fname
        with open(fpath, 'w', encoding='utf-8') as file:
            writer = csv.writer(file, dialect='unix')
            writer.writerows(summary)
        self.exporter.finish_exporting()
        self.file.close()
