# Apache Airflow 기반의 데이터 파이프라인

## CHAPTER 1 Apache Airflow 살펴보기

> Airflow는 배치 지향 데이터 파이프라인 구현을 위해 특화된 주요 기능을 가지고 있음
>
> DAG(Directed Acyclic Graph)는 Airflow의 핵심 개념이며 의존성을 정의

## CHAPTER 2 Airflow DAG의 구조

> 오퍼레이터는 단일 태스크를 나타냅니다.
>
> DAG안에 있는 태스크는 어디에 위치하든지 재시작할 수 있습니다.

## CHAPTER 3 Airflow의 스케줄링

> 하나의 스케줄 간격 작업은 해당 주기의 작업이 끝나면 시작
>
> 템플릿을 사용하여 변수를 동적으로 할당해 데이터 증분 처리가 가능
>
> @daily, timedelta, cron

- **execution_date - DAG가 실행되는 시점의 날짜와 시간을 나타냅니다.**

이전에 실패한 task를 `clear` 명령어로 재실행하거나 이전 날짜의 task를 backfill 해도
execution_date는 변경되지 않습니다.

## CHAPTER 4 Airflow 콘텍스트를 사용하여 태스크 템플릿 작업하기

> 오퍼레이터의 일부 인수는 템플릿화할 수 있습니다.
>
> 템플릿 작업은 런타임에 실행됩니다.
>
> 오퍼레이터는 **무엇**을 해야하는지 기술, 훅은 작업 **방법**을 결정

### PytionOperator

context, 및 op_kwargs, op_args를 통해서 python_callable 함수에 인자를 전달할 수 있습니다.

### Airflow Context

Airflow는 각 태스크를 **독립된 실행 단위 (task instance)**로 보고, 그때마다 context를 새로 구성

print_context 태스크에서 `context['execution_date'] = ...` 를 바꿔도,
check_execution_date 태스크는 자기 실행 시점 기준의 context만 사용합니다.

## CHAPTER 5 Airflow의 태스크 의존성

> Airflow DAG에서 선형 또는 Fan-in/out 구조를 정의할 수 있음
> BranchPythonOperator를 사용하여 DAG의 실행 경로를 동적으로 결정할 수 있음 (실행할 taskId를 return)
> 조건부 태스크를 사용하여 특정 조건에 따라 DAG에 특정 태스크를 건너뛸 수 있음

### Branching

> Branching은 DAG의 실행 경로를 동적으로 결정하는 기능입니다.
>
> return 값에 task_id를 지정하면 해당 태스크만 실행됩니다.

### Airflow Trigger Roles

> Branching 시 [task1, task2] > after_task 형태로 의존성을 정의하면
> task1 task 2가 모두 성공해야 after_task가 실행


**Airflow는 `trigger_rule` 인수를 이용해 개별 태스크에 대해 트리거 규칙을 정의합니다.**

`all_success (default)` - 모든 상위 태스크가 성공해야 해당 태스크를 실행할 수 있음

`all_failed` - 모든 상위 태스크가 실패해야 해당 태스크를 실행할 수 있음 (오류 처리 용도)

`all_done` - 상위 태스크 모두 실행 완료 여부와 관계없이 실행 (시스템 종료, 클러스트 중지)

`one_success` - 상위 태스크 중 하나라도 성공하면 실행 (다운스트림 연산/알림)

`one_failed` - 상위 태스크 중 하나라도 실패하면 실행 (알림 또는 롤백과 같은 일부 오류 처리)

`none_failed` - 상위 태스크 모두 실행 완료 또는 스킵, 실패가 없다면 실행 (조건부 브랜치의 결합)

`none_skipped` - 상위 태스크 모두 실행 완료 또는 실패, 스킵이 없다면 실행

`dummy` - 업스트림 태스크의 상태와 관계없이 항상 실행 (주로 테스트 용도)

### 조건부 태스크

> Airflow는 특정 조건에 따라 DAG에 특정 태스크를 건너뛸 수 있습니다.
>
> raise AirflowSkipException()을 사용하여 특정 조건을 만족하지 않을 때 태스크를 건너뛰도록 설정하며,
>
> [condition, task1] > after_task 형태로 의존성을 정의하여 condition을 만족할 때만 실행하도록 만들 수 있음.

- 태스크 내에서 조건

> if 문을 사용하여 특정 조건을 만족할 때만 작업을 수행하도록 설정할 수 있습니다.

**cons**

- 로직 조건이 혼용되며, `PythonOperator` 외에 다른 오퍼레이터에서는 사용 불가
- Airflow UI에서 조건부 태스크를 시각적으로 표현하기 어려움

**TO BE**

- **조건부 태스크 만들기**

정의된 조건에 따라 실행되는 테스크를 정의하며 [condition, task1] > after_task 형태로 의존성 정의

`raise AirflowSkipException()` - 특정 조건을 만족하지 않을 때 태스크를 건너뛰도록 설정

_`AirflowSkipExcetion`으로 Sikp 시에도 skipped 상태로 처리되기 때문에 `trigger_rule='none_failed'`로 설정되있다면 실행됨_

### XCom

> XCom을 사용하여 task 간에 작은 데이터를 공유할 수 있음
>
> XCom은 기본적으로 태스크 간에 메시지를 교환하여 특정 상태를 공유할 수 있게 합니다.

- **여러개의 Task에서 XCom가져오기**

```python
def f(**context):
  context['ti'].xcom_pull(
      task_ids=['task1', 'task2'],
      key='my_key',
  )
```

형태로 가져올 경우 반환되는 결과는 `[]`이며 만약 task2에서 my_key를 설정하지 않았다면
`['task1_my_value', None]` 형태로 반환됨

- **TaskId를 생략한 경우**

  현재 Task의 XCom만 가져오게 됨

- **key를 생략한 경우**

  기본키 return_value를 사용하여 가져오게 됨

### TaskFlow API

> Airflow2는 Taskflow API를 통해 파이썬 태스크 및 의존성을 정의하기 위한 새로운 Decorator 기반 API를 제공합니다.

- Taskflow 유형 태스크 간에 전달된 데이터는 XCom을 통해 자동 공유되며, XCom의 제약사항이 적용됨
- PythonOperator를 사용하여 구현되는 태스크에만 사용할 수 있음
- 다른 오퍼레이터와 함께 사용 시 의존성을 정의하는 부분에서 직관적이지 못할 수 있음

## CHAPTER 6 워크플로 트리거

> 센서(sensor)는 특정 조건이 참인지 여부를 지속적으로 확인 polling
>
> PythonSensor를 통해 True/False를 반환하는 함수로 Custom Sensor를 만들 수 있음
>
> TriggerDagRunOperator를 사용하여 다른 DAG를 트리거할 수 있음
>
> ExternalTaskSensor를 사용하여 다른 DAG의 상태를 폴링할 수 있습니다. (_성공에 따른 의존성 설정_)

### 센서를 사용한 폴링 조건

> 새로운 데이터가 도착 시 워크플로를 시작하는 방법

기존에 시간 간격을 기준으로 워크플로를 실행 시 데이터 전달 시간과 워크플로의 시작 시간 사이의 대기가 발생할 수 있음

> Airflow 오퍼레이터의 특수 타입(서브 클래스)인 센서의 도움을 받아 해결할 수 있음

- **Airflow sensor**

특정 조건이 `true`인지 지속적으로 확인하고 `true`라면 성공

만약 false인 경우, 센서는 상태가 `true`가 될 때까지, 타임아웃이 될 때까지 계속 확인

```python
from airflow.sensors.filesystem import FileSensor

wait_for_supermarket_1 = FileSensor(
    task_id="wait_for_supermarket_1",
    filepath="/opt/airflow/data/supermarket1/data.csv",  # 와일드 카드 형식 지원
)
```

- 대략 1분에 한 번씩(poke_interval) 센서는 주어진 파일이 있는지 포크(poke)합니다.
- **Poking**: 센서를 실행하고 센서 상태를 확인하기 위해 Airflow에서 사용하는 이름
- **Globbing**: 파일 이름과 경로 이름의 패턴과 일치시킬 수 있는 기능

### Sensor의 타임아웃

기본적으로 센서 타임아웃은 7일로 설정되어 있으며, schedule_interval을 하루로 설정하면,

눈덩이 효과가 발생할 수 있음

ex.) shcedule_interval을 하루로 설정하고, 센서 타임아웃을 7일로 설정

- 2025-07-10에 sensor instance가 시작
- 2025-07-11에 sensor instance가 시작
- 2025-07-11에 데이터가 들어옴 -> snsror instance 2개 모두 true를 반환하며 2번 실행됨

_*task_id등으로 멱등성을 보장해주지 않음_

**sensor mode**

poke: 대기 시간마다 포크를 수행한 후 아무 동작도 하지 않지만, 태스크 슬롯을 차지

reschedule: 대기 시간마다 포크를 수행하고, 태스크 슬롯을 차지하지 않음

### TriggerDagRunOperator

> 비슷한 기능의 태스크를 반복을 피하기 위해 여러 개의 작은 DAG로 분할하여 각 DAG가 일부 워크플로를 처리하게 할 수 있음.
>
> 다른 DAG를 트리거하기 위해 `TriggerDagRunOperator`를 사용할 수 있습니다.

- trigger_dag_id 인수에 제공되는 문자열은 트리거할 DAG의 ID와 일치해야함.
- targetDAG에 scheduled_interval이 설정되어 있더라도 즉 시 실행됨

- **PROS**
- 각 DAG가 독립적으로 실행되므로, 특정 DAG의 실패가 다른 DAG에 영향을 미치지 않음
- 각 DAG가 독립적으로 관리되기 때문에 유지보수가 쉬워지고 재사용성 향상

### DAG의 run_id

- schedulle__: DAG가 스케줄에 따라 실행될 때 생성되는 run_id
- manual__ : 사용자가 수동으로 DAG를 실행할 때 생성되는 run_id || TriggerDagRunOperator를 통해 생성되는 run_id
- backfill__ : 백필 작업에 의해 생성되는 run_id

### TriggerDagRunOperator로 백필 작업

> 태스크의 일부 로직을 변경하고 변경된 부분부터 DAG를 다시 실행하려면, 태스크를 삭제하면 됨
>
> But, 태스크 삭제는 동일한 DAG안에 태스크만 지워지며, 또 다른 DAG안에서 TriggerDagRunOperator의 다운스트림 태스크는 지워지지 않음.
>
> TriggerDagRunOperator를 삭제 시 해당 DAG가 새로 트리깅됨

### 다른 DAG의 상태 폴링하기

> 다른 DAG의 상태를 폴링하여 특정 조건이 충족되면 워크플로를 시작할 수 있습니다.
>
> `TriggerDagRunOperator`를 사용하여 다른 DAG를 실행할 경우 DAG간의 의존성이 생기지 않음. (DAG의 실행만을 담당)
>

- **다른 DAG가 실행되기 전에 여러개의 트리거 DAG가 완료 되어야하는 상황**

[dag1, dat2, dat3] >> report

_이 경우 TriggerDagRunOperator를 사용한다면 Trigger 즉 실행이 완료된 후에 report dag가 바로 실행 됨_

`ExternalTaskSensor`를 사용하여 report DAG에서 상태를 확인할 수 있음

### ExternalTaskSensor

Airflow는 다른 DAG를 고려하지 않음.

기술적으로 기본 메타데이터를 쿼리하거나 디스크에서 DAG 스크립트를 읽어서 다른 워크플로의 실행여부를 추론할 수 있지만,

직접적으로 결합되지 않음.

- 기본 동작은 자신과 **정확히 동일한 실행 날짜**를 가진 태스크에 대한 성공만 확인함.

ExternalTaskSensor가 07-01T18:00에 실행 됐다면, 07:01T18:00에 실행된 태스크의 상태만 확인함

**즉 스케줄 간격이 다르다면** ExternalTaskSensor는 다른 DAG의 태스크 **상태를 확인할 수 없음**

> Offset을 설정하여 다른 태스크를 검색할 수 있도록 설정할 수 있음 (`execution_delta`)

`execution_delta=datetime.timedelta(hours=4)` - 현재 태스크의 실행 날짜로부터 4시간 전의 태스크를 검색

`(execution_date - delta)`

### REST/CLI를 이용해서 워크플로 시작하기

> 다른 DAG에서 DAG를 Rest API 또는 CLI를 사용하여 시작할 수 있습니다.

실행 날짜가 현재 날짜 및 시간으로 설정된 dag1을 트리거 (_UI에서 수동으로 트리거하는 것과 동일_)

- **CLI**

```bash
airflow dags trigger -c '{"key": "value"}' <dag_id>
```

- **REST API**

```bash
curl -u <username>:<password> -X POST http://localhost:8080/api/v1/dags/<dag_id>/dagRuns\
-H "Content-Type: application/json" -d '{"conf": {"key": "value"}}'
```


