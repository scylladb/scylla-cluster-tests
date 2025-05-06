#!groovy

def call(){
    sh """#!/bin/bash

     ./utils/upload_sct_coredump.sh
    """
}
