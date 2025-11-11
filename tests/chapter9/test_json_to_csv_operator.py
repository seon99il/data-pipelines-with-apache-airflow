import csv
import json
from pathlib import Path

from chapter9.example_operators import JsonToCsvOperator


def test_json_to_csv_operator(tmp_path): # tmp_path는 고정으로 사용
    # given
    input_file_path = tmp_path / "input.json"
    output_file_path = tmp_path / "output.csv"

    input_data = [
        {"name": "Alice", "age": "30", "city": "New York"},
        {"name": "Bob", "age": "25", "city": "Los Angeles"},
        {"name": "Charlie", "age": "35", "city": "Chicago"},
    ]

    with open(input_file_path, 'w') as f:
        import json
        json.dump(input_data, f)

    # when
    operator = JsonToCsvOperator(
        task_id="json_to_csv",
        json_file_path=str(input_file_path),
        csv_file_path=str(output_file_path)
    )

    operator.execute(context={})
    with open(output_file_path, 'r') as f:
        reader = csv.DictReader(f)
        result = [dict(row) for row in reader]

    assert result == input_data
