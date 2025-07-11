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

>
>

### Branching

> Branching은 DAG의 실행 경로를 동적으로 결정하는 기능입니다.
>
> return 값에 task_id를 지정하면 해당 태스크만 실행됩니다.

### Airflow Trigger Roles

> Branching 시 [task1, task2] > after_task 형태로 의존성을 정의하면
> task1 task 2가 모두 성공해야 after_task가 실행


**Airflow는 `trigger_rule` 인수를 이용해 개별 태스크에 대해 트리거 규칙을 정의합니다.**

`all_success` - 모든 상위 태스크가 성공해야 해당 태스크를 실행할 수 있음

`none_failed` - 상위 태스크 모두 실행 완료 및 실패가 없다면, 실행

### 조건부 태스크

> Airflow는 특정 조건에 따라 DAG에 특정 태스크를 건너뛸 수 있습니다.

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


