import logging
from typing import Tuple

import requests
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from requests import Session


class MovielensHook(HttpHook):
    DEFAULT_HOST = 'host.docker.internal'
    DEFAULT_SCHEMA = 'http'
    DEFAULT_PORT = 5001

    def __init__(self, conn_id: str) -> None:
        super().__init__()
        self._session = None
        self._base_url = None
        self._conn_id = conn_id

    def get_conn(self) -> Tuple[Session, str]:
        """
        Builds a requests Session for the Movielens API.
        :return: A tuple of (requests.Session, _base_url)
        """
        logging.getLogger(__name__)
        if self._session is None: # get_conn 함수를 호출할 때마다 메타스토어에 작업 요청을 하지 않도록 세션이 없을 때만 생성

            config = self.get_connection(self._conn_id)
            session = requests.Session()
            schema = config.schema or self.DEFAULT_SCHEMA
            host = config.host or self.DEFAULT_HOST
            port = config.port or self.DEFAULT_PORT
            base_url = f'{schema}://{host}:{port}'
            if config.login:
                session.auth = (config.login, config.password)

            self._session = session
            self._base_url = base_url


        return self._session, self._base_url

    # 훅 사용자가 호출할 Public Method
    def get_ratings(self, start_date: str, end_date: str, batch_size: int = 100):
        """
        Fetches ratings from the Movielens API within the specified date range.
        :param start_date: Start date to query from (inclusive).
        :param end_date: End date to query up to (exclusive).
        :param batch_size: Number of records to fetch per request.
        :return: Generator yielding rating records.
        """
        session, base_url = self.get_conn()

        yield from self._get_with_pagination(
            session=session,
            url=base_url + '/ratings',
            params={'start_date': start_date.split("T")[0], 'end_date': end_date.split("T")[0]},
            batch_size=batch_size,
        )


    def _get_with_pagination(self, session: Session, url: str, params: dict, batch_size: int = 100):
        """
        Fetches records using a get request with given url/params,
        :param session:
        :param url:
        :param params:
        :param batch_size:
        :return:
        """
        session, base_url = self.get_conn()

        offset = 0
        total = None

        while total is None or offset < total:
            response = session.get(
                url, params={**params, **{'offset': offset, 'limit': batch_size}}
            )
            response.raise_for_status()
            response_json = response.json()

            yield from response_json['result']

            offset += batch_size
            total = response_json['total']


    def close(self):
        """Closes any active session."""
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None

    def __enter__(self):
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()