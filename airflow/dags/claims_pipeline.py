from datetime import datetime

from airflow import DAG
from airflow.utils import timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# =========================================================
# CONFIGURATION
# =========================================================

AWS_CONN_ID = "aws_claims"
SNOWFLAKE_CONN_ID = "snowflake_claims"

PROJECT_NAME = "CLAIMS_PIPELINE"
PIPELINE_NAME = "claims_pipeline"
ENVIRONMENT = "DEV"

S3_BUCKET = "claims-data-pipeline-2026-0808"
S3_PREFIX = "claims-data-pipeline/input/"


# =========================================================
# TASK 1
# GENERATE PIPELINE RUN CONTEXT
# =========================================================

def generate_run_context(**context):

    logical_date = context["logical_date"]

    dag_run = context["dag_run"]

    # =====================================================
    # CUSTOM RUN ID
    # =====================================================

    run_id = timezone.utcnow().strftime(
        "%Y%m%d%H%M%S"
    )

    # =====================================================
    # FILE DATE
    #
    # Default:
    #     Airflow logical date
    #
    # Optional:
    #     dag_run.conf["file_date"]
    #
    # Example:
    #
    # {
    #     "file_date": "20260808"
    # }
    #
    # This is extremely useful for manual testing.
    # =====================================================

    file_date = logical_date.strftime(
        "%Y%m%d"
    )

    dag_conf = dag_run.conf or {}

    configured_file_date = dag_conf.get(
        "file_date"
    )

    if configured_file_date:

        file_date = str(
            configured_file_date
        )

        # Validate format
        try:

            datetime.strptime(
                file_date,
                "%Y%m%d"
            )

        except ValueError:

            raise ValueError(
                "file_date must be in "
                "YYYYMMDD format. "
                f"Received: {file_date}"
            )

    # =====================================================
    # FILE NAME
    # =====================================================

    file_name = (
        f"claims_{file_date}.csv"
    )

    # =====================================================
    # S3 KEY
    # =====================================================

    s3_key = (
        f"{S3_PREFIX}{file_name}"
    )

    # =====================================================
    # COMPLETE RUN CONTEXT
    # =====================================================

    result = {

        "run_id": run_id,

        "file_date": file_date,

        "file_name": file_name,

        "s3_bucket": S3_BUCKET,

        "s3_key": s3_key
    }

    # =====================================================
    # LOG
    # =====================================================

    print("================================")
    print("PIPELINE RUN CONTEXT")
    print("================================")

    print(
        f"RUN ID       : {run_id}"
    )

    print(
        f"FILE DATE    : {file_date}"
    )

    print(
        f"FILE NAME    : {file_name}"
    )

    print(
        f"S3 BUCKET    : {S3_BUCKET}"
    )

    print(
        f"S3 KEY       : {s3_key}"
    )

    print(
        f"LOGICAL DATE : {logical_date}"
    )

    print("================================")

    return result


# =========================================================
# TASK 2
# CHECK S3 FILE
# =========================================================

def check_s3_file(**context):

    ti = context["ti"]

    # =====================================================
    # GET RUN CONTEXT
    # =====================================================

    run_context = ti.xcom_pull(
        task_ids="generate_run_context",
        key="return_value"
    )

    if not run_context:

        raise RuntimeError(
            "Pipeline run context was not found in XCom."
        )

    # =====================================================
    # GET VALUES
    # =====================================================

    s3_bucket = run_context["s3_bucket"]

    s3_key = run_context["s3_key"]

    file_name = run_context["file_name"]

    # =====================================================
    # S3 HOOK
    # =====================================================

    hook = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    exists = hook.check_for_key(
        key=s3_key,
        bucket_name=s3_bucket
    )

    # =====================================================
    # FILE NOT FOUND
    # =====================================================

    if not exists:

        raise FileNotFoundError(
            f"S3 file not found: "
            f"s3://{s3_bucket}/{s3_key}"
        )

    # =====================================================
    # SUCCESS
    # =====================================================

    print("================================")
    print("S3 FILE CHECK")
    print("================================")

    print(
        "STATUS : FILE FOUND"
    )

    print(
        f"FILE   : "
        f"s3://{s3_bucket}/{s3_key}"
    )

    print("================================")

    return {

        "status": "COMPLETED",

        "file_exists": True,

        "file_name": file_name,

        "s3_key": s3_key,

        "s3_bucket": s3_bucket,

        "message": (
            f"S3 file found successfully: "
            f"{file_name}"
        )
    }


# =========================================================
# TASK 3
# LOAD S3 → SNOWFLAKE RAW
# =========================================================

def load_raw(**context):

    ti = context["ti"]

    # =====================================================
    # GET RUN CONTEXT
    # =====================================================

    run_context = ti.xcom_pull(
        task_ids="generate_run_context",
        key="return_value"
    )

    if not run_context:

        raise RuntimeError(
            "Pipeline run context was not found in XCom."
        )

    # =====================================================
    # FILE
    # =====================================================

    file_name = run_context["file_name"]

    escaped_file_name = (
        file_name.replace(
            ".",
            r"\."
        )
    )

    # =====================================================
    # SNOWFLAKE HOOK
    # =====================================================

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # =====================================================
    # COPY COMMAND
    # =====================================================

    sql = f"""
        COPY INTO CLAIMS_DATA_DB.RAW.CLAIMS_RAW

        FROM @CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE/input/

        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )

        PATTERN = '.*{escaped_file_name}'

        ON_ERROR = 'ABORT_STATEMENT';
    """

    print("================================")
    print("LOADING S3 → RAW")
    print("================================")

    print(
        f"FILE : {file_name}"
    )

    # =====================================================
    # CONNECTION
    # =====================================================

    connection = hook.get_conn()

    cursor = connection.cursor()

    snowflake_query_id = None
    result = []

    try:

        # -------------------------------------------------
        # EXECUTE COPY
        # -------------------------------------------------

        cursor.execute(sql)

        # -------------------------------------------------
        # QUERY ID
        # -------------------------------------------------

        snowflake_query_id = cursor.sfqid

        print(
            f"SNOWFLAKE QUERY ID : "
            f"{snowflake_query_id}"
        )

        # -------------------------------------------------
        # PUSH QUERY ID IMMEDIATELY
        # -------------------------------------------------

        ti.xcom_push(
            key="snowflake_query_id",
            value=snowflake_query_id
        )

        # -------------------------------------------------
        # FETCH RESULT
        # -------------------------------------------------

        result = cursor.fetchall()

        print(
            "COPY RESULT:"
        )

        print(result)

    finally:

        cursor.close()

        connection.close()

    # =====================================================
    # CASE 1
    # ZERO FILES PROCESSED
    # =====================================================

    if (
        len(result) == 1
        and len(result[0]) == 1
        and isinstance(result[0][0], str)
        and "0 files processed" in result[0][0]
    ):

        message = (
            f"File '{file_name}' was already processed "
            f"or Snowflake found no new file to load. "
            f"COPY executed with 0 files processed."
        )

        result_data = {

            "status": "COMPLETED",

            "load_status":
                "NO_FILES_PROCESSED",

            "file_name":
                file_name,

            "rows_parsed":
                0,

            "rows_loaded":
                0,

            "errors_seen":
                0,

            "snowflake_query_id":
                snowflake_query_id,

            "message":
                message
        }

        ti.xcom_push(
            key="load_raw_result",
            value=result_data
        )

        print("================================")
        print("COPY RESULT")
        print("================================")

        print(
            "STATUS       : COMPLETED"
        )

        print(
            "LOAD STATUS  : "
            "NO_FILES_PROCESSED"
        )

        print(
            f"FILE         : "
            f"{file_name}"
        )

        print(
            "ROWS PARSED  : 0"
        )

        print(
            "ROWS LOADED  : 0"
        )

        print(
            "ERRORS SEEN  : 0"
        )

        print(
            f"MESSAGE      : "
            f"{message}"
        )

        print(
            f"QUERY ID     : "
            f"{snowflake_query_id}"
        )

        print("================================")

        return result_data

    # =====================================================
    # CASE 2
    # EMPTY RESULT
    # =====================================================

    if not result:

        message = (
            "COPY returned an empty result."
        )

        result_data = {

            "status": "COMPLETED",

            "load_status":
                "NO_RESULT",

            "file_name":
                file_name,

            "rows_parsed":
                0,

            "rows_loaded":
                0,

            "errors_seen":
                0,

            "snowflake_query_id":
                snowflake_query_id,

            "message":
                message
        }

        ti.xcom_push(
            key="load_raw_result",
            value=result_data
        )

        return result_data

    # =====================================================
    # CASE 3
    # NORMAL COPY RESULT
    # =====================================================

    copy_result = result[0]

    # -----------------------------------------------------
    # Safety check
    # -----------------------------------------------------

    if len(copy_result) < 10:

        raise RuntimeError(
            "Unexpected COPY result returned "
            f"by Snowflake: {copy_result}"
        )

    # =====================================================
    # SNOWFLAKE COPY COLUMNS
    # =====================================================

    loaded_file_name = copy_result[0]

    copy_status = copy_result[1]

    rows_parsed = copy_result[2]

    rows_loaded = copy_result[3]

    errors_seen = copy_result[5]

    first_error = copy_result[6]

    first_error_line = copy_result[7]

    first_error_character = copy_result[8]

    first_error_column_name = copy_result[9]

    # =====================================================
    # STATUS
    # =====================================================

    if copy_status == "LOADED":

        task_status = "COMPLETED"

        message = (
            "File successfully loaded into "
            "CLAIMS_DATA_DB.RAW.CLAIMS_RAW"
        )

    else:

        task_status = "FAILED"

        message = (
            f"Snowflake COPY returned status "
            f"'{copy_status}'. "
            f"First error: {first_error}"
        )

    # =====================================================
    # RESULT DATA
    # =====================================================

    result_data = {

        "status":
            task_status,

        "load_status":
            copy_status,

        "file_name":
            loaded_file_name,

        "rows_parsed":
            rows_parsed,

        "rows_loaded":
            rows_loaded,

        "errors_seen":
            errors_seen,

        "first_error":
            first_error,

        "first_error_line":
            first_error_line,

        "first_error_character":
            first_error_character,

        "first_error_column_name":
            first_error_column_name,

        "snowflake_query_id":
            snowflake_query_id,

        "message":
            message
    }

    # =====================================================
    # XCOM
    # =====================================================

    ti.xcom_push(
        key="load_raw_result",
        value=result_data
    )

    # =====================================================
    # LOG
    # =====================================================

    print("================================")
    print("COPY RESULT")
    print("================================")

    print(
        f"FILE         : "
        f"{loaded_file_name}"
    )

    print(
        f"STATUS       : "
        f"{task_status}"
    )

    print(
        f"SNOWFLAKE    : "
        f"{copy_status}"
    )

    print(
        f"ROWS PARSED  : "
        f"{rows_parsed}"
    )

    print(
        f"ROWS LOADED  : "
        f"{rows_loaded}"
    )

    print(
        f"ERRORS SEEN  : "
        f"{errors_seen}"
    )

    print(
        f"MESSAGE      : "
        f"{message}"
    )

    print(
        f"QUERY ID     : "
        f"{snowflake_query_id}"
    )

    print("================================")

    # =====================================================
    # FAILED COPY
    # =====================================================

    if task_status == "FAILED":

        raise RuntimeError(
            message
        )

    return result_data


# =========================================================
# AUDIT FUNCTION
# =========================================================

def audit_task_result(context, status):

    ti = context["ti"]

    task = context["task"]

    dag_id = context["dag"].dag_id

    task_id = ti.task_id

    # =====================================================
    # RUN CONTEXT
    # =====================================================

    run_context = ti.xcom_pull(
        task_ids="generate_run_context",
        key="return_value"
    )

    if run_context:

        run_id = run_context.get(
            "run_id"
        )

        file_name = run_context.get(
            "file_name"
        )

        source_path = (
            f"s3://"
            f"{run_context.get('s3_bucket')}/"
            f"{run_context.get('s3_key')}"
        )

    else:

        run_id = context.get(
            "run_id"
        )

        file_name = None

        source_path = None

    # =====================================================
    # TASK METADATA
    # =====================================================

    task_name = task.params.get(
        "task_name",
        task_id
    )

    step_number = task.params.get(
        "step_number"
    )

    service_name = task.params.get(
        "service_name"
    )

    environment = task.params.get(
        "environment",
        ENVIRONMENT
    )

    source_system = task.params.get(
        "source_system"
    )

    source_type = task.params.get(
        "source_type"
    )

    layer = task.params.get(
        "layer"
    )

    operation = task.params.get(
        "operation"
    )

    # =====================================================
    # TIMING
    # =====================================================

    start_time = ti.start_date

    end_time = timezone.utcnow()

    duration_seconds = None

    if start_time:

        if start_time.tzinfo is None:

            start_time = timezone.make_aware(
                start_time
            )

        duration_seconds = (
            end_time - start_time
        ).total_seconds()

    # =====================================================
    # ERROR INFORMATION
    # =====================================================

    error_code = None

    error_message = None

    if status == "FAILED":

        exception = context.get(
            "exception"
        )

        if exception:

            error_message = str(
                exception
            )

            error_code = getattr(
                exception,
                "errno",
                None
            )

    # =====================================================
    # RESULT VARIABLES
    # =====================================================

    row_count = None

    rows_parsed = None

    rows_loaded = None

    errors_seen = None

    message = None

    snowflake_query_id = None

    # =====================================================
    # GET XCOM
    # =====================================================

    if task_id == "load_raw":

        task_result = ti.xcom_pull(
            task_ids="load_raw",
            key="load_raw_result"
        )

        snowflake_query_id = ti.xcom_pull(
            task_ids="load_raw",
            key="snowflake_query_id"
        )

    else:

        task_result = ti.xcom_pull(
            task_ids=task_id,
            key="return_value"
        )

    # =====================================================
    # PROCESS RESULT
    # =====================================================

    if isinstance(
        task_result,
        dict
    ):

        rows_parsed = (
            task_result.get(
                "rows_parsed"
            )
        )

        rows_loaded = (
            task_result.get(
                "rows_loaded"
            )
        )

        errors_seen = (
            task_result.get(
                "errors_seen"
            )
        )

        message = (
            task_result.get(
                "message"
            )
        )

        row_count = rows_loaded

        result_status = (
            task_result.get(
                "status"
            )
        )

        if result_status:

            status = result_status

        # -------------------------------------------------
        # File name fallback
        # -------------------------------------------------

        if not file_name:

            file_name = (
                task_result.get(
                    "file_name"
                )
            )

        # -------------------------------------------------
        # Query ID fallback
        # -------------------------------------------------

        if not snowflake_query_id:

            snowflake_query_id = (
                task_result.get(
                    "snowflake_query_id"
                )
            )

    elif task_result is not None:

        message = str(
            task_result
        )

    # =====================================================
    # TASK-SPECIFIC MESSAGES
    # =====================================================

    if task_id == "generate_run_context":

        message = (
            f"Pipeline run context generated. "
            f"Run ID: {run_id}, "
            f"File: {file_name}"
        )

    elif task_id == "check_s3_file":

        if status == "COMPLETED":

            message = (
                f"S3 file found successfully: "
                f"{file_name}"
            )

    # =====================================================
    # INSERT AUDIT
    # =====================================================

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    sql = """
        INSERT INTO
        PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT (

            RUN_ID,
            PROJECT_NAME,
            PIPELINE_NAME,
            DAG_ID,

            STEP_NUMBER,
            TASK_ID,
            TASK_NAME,

            SERVICE_NAME,
            ENVIRONMENT,

            SOURCE_SYSTEM,
            SOURCE_TYPE,
            SOURCE_PATH,
            FILE_NAME,

            LAYER,
            OPERATION,

            STATUS,
            ROW_COUNT,
            ROWS_PARSED,
            ROWS_LOADED,
            ERRORS_SEEN,
            MESSAGE,

            START_TIME,
            END_TIME,
            DURATION_SECONDS,

            ERROR_CODE,
            ERROR_MESSAGE,

            SNOWFLAKE_QUERY_ID
        )

        VALUES (

            %s, %s, %s, %s,

            %s, %s, %s,

            %s, %s,

            %s, %s, %s, %s,

            %s, %s,

            %s, %s, %s, %s, %s, %s,

            %s, %s, %s,

            %s, %s,

            %s
        )
    """

    parameters = (

        run_id,

        PROJECT_NAME,

        PIPELINE_NAME,

        dag_id,

        step_number,

        task_id,

        task_name,

        service_name,

        environment,

        source_system,

        source_type,

        source_path,

        file_name,

        layer,

        operation,

        status,

        row_count,

        rows_parsed,

        rows_loaded,

        errors_seen,

        message,

        start_time,

        end_time,

        duration_seconds,

        error_code,

        error_message,

        snowflake_query_id
    )

    hook.run(
        sql=sql,
        parameters=parameters
    )

    # =====================================================
    # AUDIT LOG
    # =====================================================

    print("================================")
    print("AUDIT RECORD CREATED")
    print("================================")

    print(
        f"RUN_ID       : {run_id}"
    )

    print(
        f"STEP         : {step_number}"
    )

    print(
        f"TASK         : {task_id}"
    )

    print(
        f"TASK NAME    : {task_name}"
    )

    print(
        f"SERVICE      : {service_name}"
    )

    print(
        f"FILE         : {file_name}"
    )

    print(
        f"STATUS       : {status}"
    )

    print(
        f"ROWS PARSED  : {rows_parsed}"
    )

    print(
        f"ROWS LOADED  : {rows_loaded}"
    )

    print(
        f"ERRORS SEEN  : {errors_seen}"
    )

    print(
        f"MESSAGE      : {message}"
    )

    print(
        f"QUERY ID     : {snowflake_query_id}"
    )

    print(
        f"ERROR        : {error_message}"
    )

    print("================================")


# =========================================================
# SUCCESS CALLBACK
# =========================================================

def audit_success(context):

    audit_task_result(
        context,
        "COMPLETED"
    )


# =========================================================
# FAILURE CALLBACK
# =========================================================

def audit_failure(context):

    audit_task_result(
        context,
        "FAILED"
    )


# =========================================================
# DAG
# =========================================================

with DAG(

    dag_id="claims_pipeline",

    start_date=datetime(
        2026,
        1,
        1
    ),

    schedule=None,

    catchup=False,

    tags=[
        "claims",
        "aws",
        "snowflake"
    ],

) as dag:

    # =====================================================
    # STEP 1
    # =====================================================

    generate_run_context_task = PythonOperator(

        task_id="generate_run_context",

        python_callable=generate_run_context,

        on_success_callback=audit_success,

        on_failure_callback=audit_failure,

        params={

            "step_number": 1,

            "task_name":
                "Generate pipeline execution context",

            "service_name":
                "AIRFLOW",

            "environment":
                "DEV",

            "layer":
                "PIPELINE",

            "operation":
                "GENERATE_RUN_CONTEXT",
        },
    )

    # =====================================================
    # STEP 2
    # =====================================================

    check_s3_file_task = PythonOperator(

        task_id="check_s3_file",

        python_callable=check_s3_file,

        on_success_callback=audit_success,

        on_failure_callback=audit_failure,

        params={

            "step_number": 2,

            "task_name":
                "Check claims input file in S3",

            "service_name":
                "AWS_S3",

            "environment":
                "DEV",

            "source_system":
                "AWS",

            "source_type":
                "S3",

            "layer":
                "LANDING",

            "operation":
                "FILE_CHECK",
        },
    )

    # =====================================================
    # STEP 3
    # =====================================================

    load_raw_task = PythonOperator(

        task_id="load_raw",

        python_callable=load_raw,

        on_success_callback=audit_success,

        on_failure_callback=audit_failure,

        params={

            "step_number": 3,

            "task_name":
                "Load claims file from S3 into RAW",

            "service_name":
                "SNOWFLAKE",

            "environment":
                "DEV",

            "source_system":
                "AWS",

            "source_type":
                "S3",

            "layer":
                "RAW",

            "operation":
                "COPY_INTO",
        },
    )


# =========================================================
# TASK DEPENDENCIES
# =========================================================

generate_run_context_task \
    >> check_s3_file_task \
    >> load_raw_task