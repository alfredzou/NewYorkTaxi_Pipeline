from airflow.models import DagBag

def test_dagbag():
    dag_bag = DagBag(dag_folder="../dags", include_examples=False)
    assert not dag_bag.import_errors