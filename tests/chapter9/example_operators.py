from airflow.models import BaseOperator


class JsonToCsvOperator(BaseOperator):
    """
    Operator to convert a JSON file to a CSV file.

    :param json_file_path: Path to the input JSON file.
    :param csv_file_path: Path to the output CSV file.
    """

    def __init__(self, json_file_path, csv_file_path, **kwargs):
        super().__init__(**kwargs)
        self._json_file_path = json_file_path
        self._csv_file_path = csv_file_path

    def execute(self, context):
        import json
        import csv

        with open(self._json_file_path, 'r') as json_file:
            data = json.load(json_file)

        with open(self._csv_file_path, 'w', newline='') as csv_file:
            if data:
                writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)