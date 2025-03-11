from collections import defaultdict
import csv
from datetime import datetime as dt
from scrapy.exporters import CsvItemExporter

from .settings import BASE_DIR, DATETIME_FORMAT


class PepParsePipeline:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        results_dir = BASE_DIR / 'results'
        results_dir.mkdir(exist_ok=True)
        timestamp = dt.now().strftime(DATETIME_FORMAT)
        self.statuses = defaultdict(int)
        self.file = open(results_dir / f'pep_{timestamp}.csv', 'wb')
        self.exporter = CsvItemExporter(
            self.file, fields_to_export=['number', 'name', 'status']
        )

    def open_spider(self, spider):
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        item_status = item.get('status')
        if not item_status:
            raise KeyError
        if not self.statuses.get(item_status):
            self.statuses[item_status] = 1
        else:
            self.statuses[item.get('status')] += 1
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        results_dir = BASE_DIR / 'results'
        summary = [('Статус', 'Количество')]
        summary.extend(self.statuses.items())
        summary.append(('total', sum(self.statuses.values())))
        results_dir.mkdir(exist_ok=True)
        now = dt.now().strftime(DATETIME_FORMAT)
        fname = f'status_summary_{now}.csv'
        fpath = results_dir / fname
        with open(fpath, 'w', encoding='utf-8') as file:
            writer = csv.writer(file, dialect='unix')
            writer.writerows(summary)
        self.exporter.finish_exporting()
        self.file.close()
