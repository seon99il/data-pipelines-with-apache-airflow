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

## CHAPTER 7 외부 시스템과 통신하기

> 외부시스템: Airflow 및 Airflow가 구동되는 시스템 이외의 모든 기술 (S3, Spark, BigQuery)
>
> 외부 시스템의 오퍼레이터는 특정 시스템의 클라이언트를 호출하여 기능을 노출
>
> 로컬 머신에서 외부 서비스에 액세스할 수 있는 경우 `airflow tasks test <dag_id> <task_id> <execution_date>`
> 를 사용하여 태스크를 태스트 할 수 있음(Sensor의 경우 poke를 한 번만 수행하고 종료됨)
>
> BaseOperator를 상속받아 오퍼레이터를 구현할 수 있으며, 훅을 사용하여 외부 시스템과 통신할 수 있습니다.

- S3CopyObjectOperator: S3 버킷 간에 객체를 복사하는 오퍼레이터

```python
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator

download_mnist_data = S3CopyObjectOperator(
    task_id="download_mnist_data",
    source_bucket_name="source-bucket",  # From S3 버킷 이름
    source_bucket_key="mnist-data.zip",  # From File 경로
    dest_bucket_name="destination-bucket",  # To S3 버킷 이름
    dest_bucket_key="mnist-data.zip",  # To File 경로
    aws_conn_id="aws_default",  # AWS 연결 ID (Airflow Web UI에서 설정한 연결 ID)
)
```

- 특정 Task를 execution_date에 CLI로 실행할 수 있음

```bash
airflow tasks test <dag_id> <task_id> <execution_date>
```

_*Task가 Sensor인 경우, 한 번만 Poke를 수행하고 종료됨_

### 시스템 간 데이터 이동하기

> Airflow의 일반적인 사용 사례는 정기적인 ETL작업으로, 데이터를 다운하고 다른 곳으로 변환합니다.
>
> Airflow는 A-to-B 오퍼레이터를 사용할 수 있음 (`SFTPToS3Operator`, `MongoToS3Operator`)

- **MongoToS3Operator**: MongoDB에서 S3로 데이터를 이동하는 오퍼레이터

`MongoDB -> Airflow Memory -> S3` 방식이며, 메모리가 부족해질 수 있음

### 큰 작업을 외부에서 수행하기

> 오퍼레이터들은 종종 Airflow와 같은 파이썬 런타임에서 실행됨
>
> 대규모 데이터 처리 작업의 경우 AIrflow에서 실제 작업을 하는 대신, Apache Spark와 같은 처리 시스템에 작업을 위임하며,
> Airflow는 작업을 트리거하고 모니터링하는 역할을 수행할 수 있음

- SparkSubmitOperator: Apache Spark 작업을 제출하는 오퍼레이터
- SSHOperator: SSH를 통해서 Spark 작업을 실행
- SimpleHTTPOperator: HTTP 요청을 통해서 Livy(Spark REST API)를 호출하여 Spark 작업을 실행

## CHAPTER 8 커스텀 컨포넌트 빌드

> Airflow Operator가 제공하지 않는 기능을 모두 PythonOperator로 구현할 수 있지만, 재사용성이 떨어짐  
> 커스텀 오퍼레이터를 직접 쉽게 구현해 생성할 수 있으며, 이를 이용하여 지원되지 않는 시스템에서 작업을 실행할 수 있음

```shell
sh ./scripts/run_chapter8_api.sh # Movie API 실행
```

### - Hook

> Airflow에서 Hook은 외부 시스템과의 연결(Conn)을 관리하는 역할을 합니다.

- get_conn 메서드가 public이지만, 훅 외부에서 직접사용 시 외부 시스템에 액세스할 때 필요한 자세한 내부 사항을 처리하기 때문에
  이 메서드를 외부에 노출하면 캡슐화가 깨질 수 있음

### Operator

> BaseOperator 클래스를 상속하여 구현할 수 있음 (execute 메서드 오버라이드)

`@apply_defaults`: _이전의 DAG에 정의된 default_args를 오퍼레이터에 적용할 수 있도록 도와주는 데코레이터 (Deprecated)_

**원격 시스템을 포함하는 오퍼레이터**: Conn_id와 start, end, query 등을 일반적으로 포함

`template_fields = ("_start_date", "_end_date", "_output_path")`
- template_fields가 없으면 Jinja 템플릿({{ ds }} 같은 매크로)을 쓸 수 없음

- PytonOperator - op_args, op_kwargs, templates_dict 모두 template_fields에 포함되어 있음  
`template_fields: Sequence[str] = ("templates_dict", "op_args", "op_kwargs")`  
**op_kwargs**: 함수 인자와 1:1 대응을 원할 때  
**templates_dict**: **“템플릿 파일/리소스 전달용”**이라는 의미를 명확히 하고 싶을 때 사용

### 컴포넌트 패키징하기

> DAG에 커스텀 컴포넌트를 DAG 디렉터리 내에 있는 서브 패키지까지 포함했음  
> 하지만 이 방식은 다른 프로젝트에 사용하거나 공유할 경우, 이상적인 방법은 아님

- **더 나은 방법은 파이썬 패키지에 코드를 넣는 것**

이 경우 설정 시 약간의 작업이 더 필요하지만, Airflow 구성 환경에 커스텀 컨포넌트를 설치할 때에  
다른 패키지와 비슷한 방법으로 작업할 수 있다는 장점이 있음

_**목적: 구현한 훅, 오퍼레이터, 센서 클래스를 포함하는 airflow_movielens라는 패키지를 생성하는 것**_

- 디렉터리 구조

```
airflow-movielens
├── setup.py
└── src
    └── airflow_movielens
        ├── __init__.py
        ├── hooks.py
        ├── operators.py
        ├── ranking.py
        └── sensors.py
```

- 설치

```shell
pythom -m install ./airflow-movielens
```

- **find_packages("src")**는 어떤 이름의 패키지(airflow_movielens)를 빌드해야 하는지 setuptools에게 알려줍니다.

- **package_dir={"": "src"}**는 그 패키지들의 실제 코드가 프로젝트의 src/ 디렉토리에 있다는 위치 정보를 알려줍니다.

```python
from airflow_movielens.sensors import MovielensRatingsSensor  # 다른 package 처럼 import 가능

...

ratings_sensor = MovielensRatingsSensor(
    task_id="wait_for_ratings_data",
    filepath="/opt/airflow/data/ratings.csv",
)

...
```

`src/` 안에 package를 스캔해서 `lib/{package_name}` 형태로 설치됨

`uv run pip install -e ./airflow-movielens`로 설치 시 패키지 내 코드를 수정할 때마다 자동으로 반영

_pyproject.toml 파일을 사용하여 정의할 수 있음 (uv add 가능)_

### 요약

- 사용자는 특정 사례에 맞는 커스텀 컴포넌트를 구현하여, Airflow의 기능을 확장할 수 있음
- 커스텀 훅을 사용하여 지원하지 않는 시스템과 연동할 수 있음
- 개별 워크플로에 특화되거나 Airflow 기본 내장 오퍼레이터로 처리할 수 없는 컴포넌트를 구현할 수 있음
- 커스텀한 오퍼레이터, 훅, 센서 등의 코드들을 (배포가능한) 파이썬 라이브러리로 구현하여 보다 구조적으로 만들 수 있음

## CHAPTER 9 Airflow Testing

- CI/CD 파이프라인에서 Airflow 테스트하기
- pytest로 테스트하기 위한 프로젝트 구성하기
- 템플릿을 적용한 테스트 태스크를 위한 DAG 실행 시뮬레이션하기
- Mocking으로 외부 시스템 조작하기
- 컨테이너를 사용하여 외부 시스템에서 동작 테스트하기

개별 태스크 단위의 단위 테스트,  
여러 구성 요수의 동작을 함께 검증하는 통합 테스트

- pytest, github action

### 모든 DAG에 대한 모결성 검증

> 첫 번째 단계는 일반적으로 DAG 무결성 테스트, 즉 모든 DAG가 정상적으로 구현되었는지 검증

### Test with Pytest

> 일반적으로 ~/tests 디렉터리에 테스트 코드를 배치하며 검사 대상 코드를 그대로 복사하여 구성

모든 파일 이름을 그대로 따르며 test_*.py 형태로 prefix를 붙임

#### Mock

- `pip install pytest-mock` - pytest-mock 플러그인 설치 `def test_example(mocker):`로 mock 객체를 주입받아 사용

> mocker 라는 인수를 테스트 한수에 전달해서 사용할 수 있음

_사용자가 직접 인수를 입력하고 싶다면 pytest_mock.MockFixture를 입력하여 사용할 수 있음_

목업 객체의 가장 큰 함정은 잘못된 객체를 목업으로 구현하는 것

get_connection은 BaseHook에서 제공하는 메서드이지만 상속된 MovielensHook에서 오버라이드함  
파이썬에서 목업을 구현하는 올바른 방법은 정의되는 위치가 아니라 호출되는 위치에서 목업을 구성하는 것

### 디스크의 파일로 테스트하기

> JSON 리스트를 가진 파일을 읽고 이를 CSV형식으로 쓰는 오퍼레이터가 있다면,
> JSON 파일을 읽는 부분을 모킹하여 사용할 수 있음

- 파이썬에는 임시 저장소와 관련된 작업을 위한 `tempfile` 모듈이 포함되어 있음

사용 후 디렉터리와 내용이 지워지기 때문에 파일 시스템에 해당 항목이 남지 않음  
pytest는 tmp_dir(os.path 객체) 및 tmp_path(pathlib 객체)라는 tempfile 모듈에 대한 편리한 사용 방법을 제공

#### pyTest - Fixture

> pytest의 Fixture 기능을 사용하여 테스트에 공통으로 필요한 리소스를 미리 준비할 수 있음


### 테스트에서 DAG 및 태스크 콘텍스트로 작업하기
> 일부 오퍼레이터는 실행을 위해 더 많은 Context 또는 작업 Instance가 필요할 수 있음

오퍼레이터 실행 단계:

1. 태스크 인스턴스 콘텍스트 구성 (모든 변수를 수집)
2. 현재 테스크 인스턴스에 대한 XCom 데이터 삭제 -> Airflow MetaStore
3. 템플릿 변수 구체화
4. operator.pre_execute() 호출
5. operator.execute() 호출
6. XCom에 값 저장 -> Airflow MetaStore
7. operator.post_execute() 호출

- 템플릿을 사용하면 태스크 인스턴스 콘텍스트를 실행해야 하므로 이전과 같이 단순히   
  오퍼레이터의 execute 메서드를 호출하는 것만으로는 테스트할 수 없음

이를 위해서 Airflow 자체에서 태스크를 시작할 때 사용하는 실제 메서드인 `operator.run()`을 호출  
이를 사용하려면 DAG에 오퍼레이터를 할당해야 함

```python
    task.run(
    start_date=test_dag.default_args["start_date"],
    end_date=test_dag.default_args["start_date"],
    ignore_ti_state=True,  # 꼭 넣어서 실행할 것
)
```

datetime.datetime(2019, 10, 10)라면 `start_date`와 `end_date`를 동일하게 설정하여 단일 실행

- context['date_interval_start'] = DateTime(2019, 10, 10, 0, 0, 0, tzinfo=Timezone('UTC'))
- context['date_interval_end'] = DateTime(2019, 10, 11, 0, 0, 0, tzinfo=Timezone('UTC'))

start: datetime.datetime(2019, 10, 10) / end: datetime.datetime(2019, 10, 11)로 잡으면?
10일부터 11일 사이에 execution_date가 (10일, 11일) 2개가 존재하므로 2번 실행됨

```python
# BaseOperator.run() 내부
for info in self.dag.iter_dagrun_infos_between(start_date, end_date, align=False):
```

에 따라서 2번 실행

- Task1
    - context['date_interval_start'] = DateTime(2019, 10, 10, 0, 0, 0, tzinfo=Timezone('UTC'))
    - context['date_interval_end'] = DateTime(2019, 10, 11, 0, 0, 0, tzinfo=Timezone('UTC'))

- Task2
    - context['date_interval_start'] = DateTime(2019, 10, 11, 0, 0, 0, tzinfo=Timezone('UTC'))
    - context['date_interval_end'] = DateTime(2019, 10, 12, 0, 0, 0, tzinfo=Timezone('UTC'))

`schedule_interval="@weekly"` 일때면? 1번 실행 (start: 2019-10-10 / end: 2019-10-11)

`schedule_interval="*/10 * * * *"`(10분) 일때면? 

- `context['data_interval_start'] = {DateTime} DateTime(2019, 10, 10, 0, 0, 0, tzinfo=Timezone('UTC'))`  
  `context['data_interval_end'] = {DateTime} DateTime(2019, 10, 11, 0, 0, 0, tzinfo=Timezone('UTC'))`
- `context['data_interval_start'] = {DateTime} DateTime(2019, 10, 10, 0, 10, 0, tzinfo=Timezone('UTC'))`  
  `context['data_interval_end'] = {DateTime} DateTime(2019, 10, 10, 0, 20, 0, tzinfo=Timezone('UTC'))`
- `context['data_interval_start'] = {DateTime} DateTime(2019, 10, 10, 0, 20, 0, tzinfo=Timezone('UTC'))`  
  `context['data_interval_end'] = {DateTime} DateTime(2019, 10, 10, 0, 30, 0, tzinfo=Timezone('UTC'))`
- _... 10분 단위로 계속 반복됨_ (`10일 00시 ~ 11일 00시`로 실행된 후 `10일 00시10분 ~ 10일 00시20`분 형태로 계속 실행)

_10일00:00 부터 11일00:00실행 후 interval에 맞게 실행됨_
### DAG 완료 테스트하기

DAG의 모든 오퍼레이터가 예상한 대로 작동하려면 어떻게 해야할까요?  
A: 다양한 이유로 실제 환경을 시뮬레이션하는 것이 항상 가능하지 않음, 프로덕션 환경에서 데이터 크기를 고려하면  
DTAP (Dev, Test, Acceptance, Prod) 환경 모두 데이터를 Sync하는 것은 비현실적임

#### Whirl을 이용한 프로덕션 환경 에뮬레이션

https://github.com/godatadriven/whirl

#### DTAP 환경 생성

> 도커를 활용한 로컬에 운영 환경을 시뮬레이션 하거나, Dev, Prod 브랜치로 관리하기

### 요약

- DAG 무결성 테스트는 DAG의 기본 오류를 필터링함
- 단위 테스트를 통해 개별 오퍼레이의 정확성을 검증
- Pytest 및 플러그인은 테스트를 위해, 임시 디렉터리, 도커 컨테이너, 목업 객체 등을 쉽게 생성할 수 있도록 도와줌
- 태스크 인스턴스 콘텍스트를 사용하지 않는 오퍼레이터는 `execute()`로 쉽게 테스트할 수 있음
- 태스크 인스턴스 콘텍스트를 사용하는 오퍼레이터는 DAG와 함께 실행되어야 함 (task.run())
- 통합 테스트를 위해서는 프로덕션 환경과 최대한 비슷하게 시뮬레이션해야하며, 환경을 분리 Copy해서 관리하는 것이 중요함

## CHAPTER 10 컨테이너에서 태스크 실행하기

- Airflow 배포관리와 관련된 몇가지 문제를 파악하기
- 컨테이너 접근 방식이 Airflow 배포에 어떻게 도움이 되는지 검토
- 도커의 Airflow에서 컨테이너화된 태스크 실행학디
- 컨테이너화된 DAG 개발에서 워크플로에 대한 전반적인 개요 수립

### 다양한 오퍼레이터를 쓸 때 고려해야할 점

> 여러 오퍼레이터가 있는 DAG는 복잡하기 때문에 만들고 관리하는 것이 어려움

- `Chapter8 HttpOperator >> PythonOperator >> MysqlOperator`: 각 단계에서 서로 다른 오퍼레이터가 포함되므로 DAG의  
  개발 및 유지 관리가 복잡해짐

- Problem1: 각 오퍼레이터별로 인터페아스와 내부 동작에 익숙해져야 한다는 단점 (러닝 커브 증가)
- 다양한 종속성이 충돌하는 환경: HttpOperator는 requests에 종속적이며, MySQLOperator는 mysql-connector-python에 종속적임  
  → Airflow 설정 방식 때문에 모둔 종속성을 Airflow, 워커 자체에도 설치해야 함

_또 한 잠재적인 보안 위험과 충돌이 발생할 수 있으며, 동일한 환경에 동일한 패키지를 여러 버전 설치할 수 없음_

### 제네릭 오퍼레이터 지향하기

> 일부에서는 하나의 제너릭 오퍼레이터를 사용하여 태스크를 실행하는 방식이 더 낫다고 주장하기도 함

- **장점**
    - 단일 오퍼레이터만 이해하면 되므로 러닝 커브가 낮아짐
    - 오퍼레이터별로 종속성을 설치할 필요가 없음
    - 오퍼레이터의 내부 동작이 단순해짐
    - 단일 오퍼레이터에 필여한 하나의 Airflow 종속성 집합만 관리하면 됨

각각에 대한 종속성 패키지를 설치하고 관리하지 않고도 동시에 다양한 태스크를 실행할 수 있는   
제네릭 오퍼레이터는 컨테이너를 활용하면 가능함

#### Docker Container with volumes

```shell
docker run --rm -v `pwd`/data:/data my-docker-image
```

### Airflow를 통해 태스크를 컨테이너로 실행

> ex.) DockerOperator, KubernetesPodOperator

Chapter8: DAG (DockerOperator >> DockerOperator >> DockerOperator)  
_Docker 내에서 HttpContainer, PythonContainer, MySQLContainer를 사용_

생각보다 복잡함, 컨테이너를 만들고 관리하는 작업이 추가됨

Airflow에서는 태스크의 의존성, 실행 주기관리만을 담당하고 실제 테스크의 실행 로직을 분리할 수 있음 (or Spark)

- **장점**
    - **간편한 종속성 관리:**  
      서로 다른 이미지를 생성하면 각 태스크에 필요한 종속성 충돌이 발생하지 않음
    - **향상된 테스트 가능성:**  
      Airflow DAG와는 별도로 개발 및 유지 관리할 수 있음
    - **다양한 태스크 실행 시에도 동일한 접근방식을 제공:**  
      동일한 인터페이스를 제공하며, 오퍼레이터 관련 문제를 줄여줌

```Dockerfile
FROM python:3.8-slim


RUN python -m pip install click==7.1.1 requests==2.23.0

COPY scripts/fetch_ratings.py /usr/local/bin/fetch-ratings
RUN chmod +x /usr/local/bin/fetch-ratings

# 컨테이너 내에서 fetch-ratings 명령어를 사용할 수 있도록 PATH 설정
ENV PATH="/usr/local/bin:${PATH}"
```

```python
fetch_ratings = DockerOperator(
    task_id="fetch_ratings",
    image="manning-airflow/movielens-fetch",
    auto_remove="force",
    mount_tmp_dir=False,
    command=[
        "fetch-ratings",
        "--start_date",
        "{{ds}}",
        "--end_date",
        "{{next_ds}}",
        "--output_path",
        "/data/ratings/{{ds}}.json",
        "--user",
        "airflow",
        "--password",
        "airflow",
        "--host",
        "http://host.docker.internal:5001",
    ],
    network_mode="airflow",
    mounts=[
        Mount(
            source="/tmp/airflow/data",  # 호스트 경로
            target="/data",  # 컨테이너 경로
            type="bind",  # 바인딩 마운트 타입 지정
        )
    ],
)
```

- `/var/run/docker.sock:/var/run/docker.sock`
    - 도커 컨테이너 안에서 Localhost의 도커 데몬에 접근하기 위한 소켓 바인딩

1. 필요한 이미지에 대한 도커 파일 생성 및 이미지를 빌드
2. 도커 데몬은 개발 머신에 해당하는 이미지를 구축
3. 도커 데몬은 이미지를 나중에 사용할 수 있도록 컨테이너 레지스트리에 게시
4. 개발자는 빌드 이미지를 참조하여 DAG를 작성
5. DAG 활성화된 후 Airflow는 DAG 실행을 시작하고 각 실행에 대한 DockerOperator 태스크를 스케줄함
6. Airflow 워커는 DockerOperator 태스크를 선택하고 컨테이너 레지스트리에서 필요한 이미지를 로드
7. 워커에 설치된 도커 데몬을 사용하여 해당 이미지와 인스로 컨테이너를 실행

### 쿠버네티스에서 태스크 실행

도커는 컨테이너화된 태스크를 단일 시스템에서 실행할 수 있는 편리한 접근 방식을 제공함  
하지만 여러 시스템에서 태스크를 조정하고 분산하는 데는 어려움이 있음 → 쿠버네티스가 해결책이 될 수 있음



#### 도커 기반 워크플로와 차이점

- 태스크 컨테이너가 더 이상 워커 노드에서 실행되지 않고 k8s 클러스터 내에 별도의 노드에서 실행됨
  즉 워커에 사용되는 모든 리소스는 최소화되며, k8s의 기능을 사용하여 적절한 리소스가 있는 노드에 태스크가 배포됨
- 어떤 스토리지도 더 이상 Airflow 워커가 접근하지 않지만, 쿠버네티스 파드에서는 사용할 수 있어야 함

전반적으로 쿠버네티스는 도커에 비해 확장성, 유연성 및 스토리지, 보안 등과 같은 리소스 관리 관점에서 상당한 이점을 가지고 있음


### 요약
- Airflow 배포는 다양한 API에 대한 지식이 필요하고 디버깅 및 종속성 관리가 복잡하기 때문에 여러 오퍼레이터가 함께 운영되는 경우
    관리가 어려울 수 있음
- 도커와 같은 컨테이너 기술을 사용하여 이미지 내부의 태스크를 캡슐화하고 Airflow 내에세 해당 이미지를 실행하는 방법이 가능
- 컨테이너화된 접근 방식은 종속성 관리, 태스크 운영을 위한 일관된 인터페이스, 테스크 테스트의 향상 등 여러가지 이점이 있음
- DockerOperator를 사용하면 docker run CLI 명령과 유사하게 도커를 사용하여 컨테이너 이미지에서 태스크를 직접 실행할 수 있음
- KubernetesPodOperator를 사용하면 쿠버네티스 클러스터 내 파드에서 태스크를 실행할 수 있음

