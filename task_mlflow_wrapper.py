import prefect
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_link_artifact
import mlflow
import datetime
import functools
import inspect

def task_with_mlflow(mlflow_server_uri = "10.5.1.218:8888", artifact_dir = None, 
        arg_name_artifact_dir_before_exec = None, arg_name_artifact_dir_after_exec = None):
    def prefect_task_wrapper(mlflow_server_uri, exp_id, run_id):
        def prefect_task_wrapper2(f):
            # This wrapper function is for the logging of the mlflow url.
            @task
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                experiment_url = f"http://{mlflow_server_uri}/#/experiments/{exp_id}/runs/{run_id}"
                description = "{} exp_id: {} run_id: {}".format(f.__name__, exp_id, run_id)
                create_link_artifact(key = "mlflow", link = experiment_url, description = description)
                ret = f(*args, **kwargs) 
                return ret
            return wrapper
        return prefect_task_wrapper2

    def task_with_mlflow_wrapper(f):
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

                if arg_name_artifact_dir_before_exec != None:
                    #if not isinstance(arg_name_artifact_dir_before_exec, str):
                    #    raise
                    if arg_name_artifact_dir_before_exec in bound_args.arguments:
                        artifact_dir = bound_args.arguments[arg_name_artifact_dir_before_exec]
                        if artifact_dir != None:
                            mlflow.log_artifacts(artifact_dir, artifact_path = "artifacts_before_exec")
                            print("save1 done")

                #----------------------------------------
                # Execution
                #----------------------------------------
                decorate_func = prefect_task_wrapper(mlflow_server_uri = mlflow_server_uri, exp_id = experiment_id, run_id = run_id)(f)
                ret_value = decorate_func(*args, **kwargs)
                #----------------------------------------
                # Prefect: Save the log 
                #----------------------------------------
                experiment_url = f"http://{mlflow_server_uri}/#/experiments/{experiment_id}/runs/{run_id}"
                logger = get_run_logger()
                logger.info(f"{function_name}: {experiment_url}")
                #----------------------------------------
                # MLFlow: save all artifact
                #----------------------------------------
                if arg_name_artifact_dir_after_exec != None:
                    #if not isinstance(arg_name_artifact_dir_after_exec, str):
                    #    raise
                    if arg_name_artifact_dir_after_exec in bound_args.arguments:
                        artifact_dir = bound_args.arguments[arg_name_artifact_dir_after_exec]
                        if artifact_dir != None:
                            mlflow.log_artifacts(artifact_dir, artifact_path = "artifacts_after_exec")
                            print("save2 done")
                #if artifact_dir != None:
                #    mlflow.log_artifacts(artifact_dir, artifact_path = "artifacts")
                task_desc = dict()
                task_desc["inputs"] = bound_args.arguments
                task_desc["output"] = ret_value
                mlflow.log_dict(task_desc, "log.json")
                
                if isinstance(ret_value, float):
                    mlflow.log_metric(function_name, ret_value)
            return ret_value 
        return _wrapper
    return task_with_mlflow_wrapper

#def for_link(f):
#    @functools.wrapper(f)
#    def wrapper(*args, **kwargs):
#        info = kwargs.pop("info")
#        link = f"fuga//{info.run_id}"
#        create_link_artifact(link)
#        return f(*args, **kwargs)
#    return wrapper
#
#@mlflow(artifacts_dir)
#@for_link
#def str_twice(s):
#    return s * s
#str_twice(s, link="fuga")


@task_with_mlflow(arg_name_artifact_dir_before_exec = "artifact_dir", arg_name_artifact_dir_after_exec = "artifact_dir")
def str_twice(s, artifact_dir = None):
    return s*2


@task_with_mlflow()
def str_triple(s):
    return s*3

@task_with_mlflow()
def str_add(s1, s2):
    #create_link_artifact(link = "www.google.com", key = "yahoo", description = "yahoo description")
    return s1+s2

@task_with_mlflow()
def str_and(s1,s2):
    return s1 and s2

@flow()
def my_test(a = "hello", b = "byebye"):
    a = str_twice(a, artifact_dir = "./hoge")
    aa = str_twice(a)
    bb = str_twice(b)
    ret = str_add(aa,bb)
    print(ret)

my_test()
