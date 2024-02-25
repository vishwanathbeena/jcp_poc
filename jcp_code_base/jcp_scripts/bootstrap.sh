#!/bin/bash
mkdir -p /home/hadoop/jcp_pipeline
aws s3 cp s3://vishwa-poc-jcp/jcp_pipeline/ /home/hadoop/jcp_pipeline/ --recursive
