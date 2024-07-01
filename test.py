import prefect
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_link_artifact
import mlflow
import datetime
import functools
import inspect

def my_logger(f, mlflow_server_uri = "10.5.1.218:8888"):
    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        
        #----------------------------------------
        # Get Function Name and Arguments
        #----------------------------------------
        function_name = f.__name__
        signature = inspect.signature(f)
        param_names = [param.name for param in signature.parameters.values()]
        bound_args = signature.bind(*args, **kwargs)
        bound_args.apply_defaults()

        #decorate_func = task(f, log_prints = True)
        decorate_func = task(f)
        #----------------------------------------
        # Set up MLFlow
        #----------------------------------------
        mlflow.set_experiment(function_name)
        with mlflow.start_run() as run:
            run_id = run.info.run_id
            experiment_id = run.info.experiment_id

            #----------------------------------------
            # MLFlow: Save the input parameters 
            #----------------------------------------
            for name, value in bound_args.arguments.items():
                mlflow.log_param(name, value)

            #----------------------------------------
            # Execution
            #----------------------------------------
            ret_value = decorate_func(*args, **kwargs)

            #----------------------------------------
            # Prefect: Save the log 
            #----------------------------------------
            experiment_url = f"http://{mlflow_server_uri}/#/experiments/{experiment_id}/runs/{run_id}"
            logger = get_run_logger()
            logger.info(f"MLflow Experiment URL: {experiment_url}")
            create_link_artifact(key = "function-link", link = experiment_url, description = "{} exp_id: {} run_id: {}".format(function_name, experiment_id, run_id))

            #----------------------------------------
            # MLFlow: save all artifact
            #----------------------------------------
            #if artifact_path != None:
            #    mlflow.log_artifacts(artifact_path)
            
            if isinstance(ret_value, float):
                mlflow.log_metric(function_name, ret_value)
        return ret_value 
    return _wrapper


@my_logger
def str_twice(s):
    return s*2

@my_logger
def str_triple(s):
    return s*3

@my_logger
def str_add(s1, s2):
    create_link_artifact(link = "www.google.com", key = "yahoo", description = "yahoo description")
    return s1+s2

@my_logger
def str_and(s1,s2):
    return s1 and s2

@flow
def my_test(a = "hello", b = "byebye"):
    aa = str_twice(a)
    bb = str_twice(b)
    ret = str_add(aa,bb)
    print(ret)

my_test()
