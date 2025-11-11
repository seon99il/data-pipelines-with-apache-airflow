from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class MovielensRatingsSensor(BaseSensorOperator):
    """
    Sensor to check for the availability of new ratings data in the Movielens API.


    :method:`poke`: Checks if new ratings data is available. return True if available, False otherwise.

    """

    template_fields = ("_start_date", "_end_date")


    def __init__(self,
        conn_id: str,
        start_date: str = "{{ ds }}",
        end_date: str = "{{ next_ds }}",
        **kwargs):
        self._start_date = start_date
        self._conn_id = conn_id
        self._end_date = end_date
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        from custom.hooks import MovielensHook
        hook = MovielensHook(conn_id=self._conn_id)

        self.log.info("Poking for new ratings data from %s to %s", self._start_date, self._end_date)
        try:
            next(
                hook.get_ratings(start_date=self._start_date, end_date=self._end_date)
            )

            self.log.info("Poking for new ratings data is available")
            return True
        except StopIteration: # 데이터가 없을 때 발생
            self.log.info("No new ratings data available yet")
            return False
        finally:
            hook.close()

