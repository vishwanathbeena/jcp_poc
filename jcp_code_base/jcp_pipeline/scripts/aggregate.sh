#!/bin/bash
spark-submit --master local --deploy-mode client /home/hadoop/jcp_pipeline/src/app.py --config_file_path '/home/hadoop/jcp_pipeline/config/common_config.json' --process_name 'aggregate'