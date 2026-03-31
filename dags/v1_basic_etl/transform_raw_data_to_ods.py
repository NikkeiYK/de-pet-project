from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta


CONN_NAME = "postgres_dwh"

with DAG(
    dag_id="trancform_raw_data_to_ods",
    schedule="@once",
    tags=["basic", "transform"],
    catchup=False,
    start_date=datetime(2026, 3, 31)
) as dag:
    start = EmptyOperator(task_id="start_transform_stage")
    
    sensor = ExternalTaskSensor(
        task_id="wait_data_generation",
        external_dag_id="basic_etl_data_generation",
        allowed_states=["success", "failed"],
        poke_interval=15,
        mode="reschedule",
        timeout=1440,
        
    )
    
    reactors = SQLExecuteQueryOperator(
        task_id="load_raw_reactors_to_ods",
        conn_id=CONN_NAME,
        sql=f'''
            create table if not exists ods.ods_reactors (
                reactor_id serial primary key,
                reactor_name varchar(90),
                type varchar(50),
                max_temperature decimal,
                max_pressure decimal,
                installation_date timestamp,
                last_maintenance timestamp,
                status varchar(50),
                loaded_at timestamp default now()
            );
            
            truncate table ods.ods_reactors cascade;
            
            insert into ods.ods_reactors
            select 
                reactor_id,
                reactor_name,
                type,
                max_temperature,
                max_pressure,
                installation_date::timestamp,
                last_maintenance::timestamp,
                status
            from raw.raw_reactors;
        '''
    )
    
    batches = SQLExecuteQueryOperator(
        task_id="load_raw_batches_to_ods",
        conn_id=CONN_NAME,
        sql=f'''
            create table if not exists ods.ods_batches (
                batch_id serial primary key,
                reator_id integer references ods.ods_reactors(reactor_id),
                product_name varchar(50),
                targetr_quantity_tons decimal,
                actual_quantity_tons decimal,
                process_temp_avg decimal,
                process_pressure_avg decimal,
                catalyst_id integer,
                batch_start timestamp,
                batch_end timestamp,
                status varchar(50),
                load_at timestamp default now()
            );
            
            truncate table ods.ods_batches cascade;
            
            insert into ods.ods_batches
            select 
                batch_id,
                reactor_id,
                product_name,
                target_quantity_tons::decimal,
                actual_quantity_tons::decimal,
                process_temp_avg::decimal,
                process_pressure_avg::decimal,
                catalyst_id,
                batch_start::timestamp,
                batch_end::timestamp,
                status
            from raw.raw_batches;
        '''
    )
    
    
    events = SQLExecuteQueryOperator(
        task_id="load_raw_events_to_ods",
        conn_id=CONN_NAME,
        sql=f'''
            create table if not exists ods.ods_downtime_events (
                event_id serial primary key,
                reactor_id integer references ods.ods_reactors(reactor_id),
                reason varchar(50),
                start_time timestamp,
                end_time timestamp,
                duration_hours decimal,
                lost_production_tons decimal,
                estimated_loss_rub decimal,
                load_at timestamp default now()
            );
            
            truncate table ods.ods_downtime_events cascade;
            
            insert into ods.ods_downtime_events
            select 
                event_id,
                reactor_id,
                reason,
                start_time::timestamp,
                end_time::timestamp,
                duration_hours::decimal,
                lost_production_tons::decimal,
                estimated_loss_rub::decimal
            from raw.raw_downtime_events;
        '''
    )
    
    tasks = SQLExecuteQueryOperator(
        task_id="load_raw_tasks_to_ods",
        conn_id=CONN_NAME,
        sql=f'''
            create table if not exists ods.ods_quality_tests (
                test_id serial primary key,
                batch_id integer references ods.ods_batches(batch_id),
                parameter varchar(50),
                measured_value decimal,
                spec_min decimal,
                spec_max decimal,
                test_method varchar(50),
                lab_date timestamp,
                passed boolean,
                load_at timestamp default now()
            );
            
            truncate table ods.ods_quality_tests cascade;
            
            insert into ods.ods_quality_tests
            select 
                test_id,
                batch_id,
                parameter,
                measured_value::decimal,
                spec_min::decimal,
                spec_max::decimal,
                test_method,
                lab_date::timestamp,
                passed
            from raw.raw_quality_tests;
        '''
    )
    
    telemetry = SQLExecuteQueryOperator(
        task_id="load_raw_telemetry_to_ods",
        conn_id=CONN_NAME,
        sql=f'''
            create table if not exists ods.ods_sensor_telemetry (
                reading_id serial primary key,
                reactor_id integer references ods.ods_reactors(reactor_id),
                batch_id integer references ods.ods_batches(batch_id),
                sensor_type varchar(50),
                value decimal,
                unit varchar(50),
                timestamp timestamp,
                load_at timestamp default now()
            );
            
            truncate table ods.ods_sensor_telemetry cascade;
            
            insert into ods.ods_sensor_telemetry
            select 
                reading_id,
                reactor_id,
                batch_id,
                sensor_type,
                value::decimal,
                unit,
                timestamp::timestamp
            from raw.raw_sensor_telemetry
            where is_anomaly = False and batch_id is not null
            order by batch_id;
        '''
    )
    
    end = EmptyOperator(task_id="end_transform_stage")
    
    start >> sensor >> reactors >> batches >>  [events, tasks, telemetry] >> end