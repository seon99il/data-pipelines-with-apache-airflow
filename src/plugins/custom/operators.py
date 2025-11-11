import json
import os.path
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context




class  MovielensFetchRatingsOperator(BaseOperator):

    # 커스텀 오퍼레이터에서 템플릿화할 인스턴스 변수들을 지정 Jinja 템플릿 형태로 값이 들어오면 값을 변환
    template_fields = ("_start_date", "_end_date", "_output_path")
    # 없으면 - date: {{ ds }} to {{ next_ds }} string 으로 출력됨
    """
    Operator that fetches movie ratings from an external API and saves them to a specified output path.

    :param conn_id: The connection ID for the external API.
    :param output_path: The file path where the fetched ratings will be saved.
    :param start_date: The start date for fetching ratings (default is "{{ ds }}").
    :param end_date: The end date for fetching ratings (default is "{{ next_ds }}").
    """
    def __init__(self,
        conn_id: str,
        output_path: str,
        start_date: str = "{{ ds }}",
        end_date: str = "{{ next_ds }}",
        **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._output_path = output_path
        self._start_date:str  = start_date
        self._end_date:str = end_date

    def execute(self, context: Context) -> Any:
        from custom.hooks import MovielensHook
        hook = MovielensHook(conn_id=self._conn_id)

        self.log.info("Context: " + str(context))

        self.log.info("date: " + str(self._start_date) + " to " + str(self._end_date))
        

        try:
            self.log.info(f"Fetching movie ratings for {self.start_date} to {self._end_date}")

            ratings = list(
                hook.get_ratings(self._start_date, self._end_date)
            )

            self.log.info(f"Fetched {len(ratings)} ratings")
        finally:
            hook.close()

        self.log.info(f"Writing ratings to {self._output_path}")

        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self._output_path, "w") as f:
            json.dump(ratings, f)


