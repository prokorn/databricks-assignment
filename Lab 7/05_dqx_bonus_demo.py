# Databricks Labs DQX Demo
%pip install databricks-labs-dqx --quiet
dbutils.library.restartPython()

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null, is_not_null_and_not_empty
from databricks.sdk import WorkspaceClient

engine = DQEngine(WorkspaceClient(), spark, observer=DQMetricsObserver())

# Define rules via DQX syntax
checks = [
    DQRowRule(name="valid_title_dqx", criticality="error", check_func=is_not_null_and_not_empty, column="title"),
    DQRowRule(name="valid_movie_id_dqx", criticality="error", check_func=is_not_null, column="movie_id")
]

df = spark.table("main.lab_data.dim_movies")
result = engine.apply_checks(df, checks)
validated_df = result[0] if isinstance(result, tuple) else result

display(engine.compute_summary_metrics(validated_df))