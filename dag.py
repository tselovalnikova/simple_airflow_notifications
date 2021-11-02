from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowFailException


def raise_exception():
    raise AirflowFailException("Permanent exception")


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection('slack').password
    slack_msg = """
            :green_circle: Task Failed, let's retry. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'on_failure_callback': task_fail_slack_alert
}


dag = DAG(
    dag_id="slack_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

t1 = PythonOperator(task_id='t1', python_callable=raise_exception, dag=dag)
