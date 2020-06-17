#!/usr/bin/env bash

PY_VERSION=3.7.7

create_virtualenv_local(){
    PY_VERSION=$1
    BASE_PATH=$(basename `pwd`)
    pyenv install $PY_VERSION -s
    pyenv local $PY_VERSION && \
    python --version && \
    python -m venv .venv && \
    ln -s `pwd`/.venv ~/.pyenv/versions/$BASE_PATH && \
    pyenv local $BASE_PATH
    echo "Python virtual env created successfully"
}

# check to see if pipenv is installed
if [ -x "$(which pyenv)" ]
then

    # create virtuelenv local if it does not exist
    test -d "$(pwd)/.venv" || create_virtualenv_local $PY_VERSION

    # install packages to a temporary directory and zip it
    pip install -r requirements.txt --target ./packages

    # check to see if there are any external dependencies_
    # if not then create an empty file to seed zip with
    if [ -z "$(ls -A packages)" ]
    then
        touch packages/empty.txt
    fi

    # zip dependencies_
    if [ ! -d packages ]
    then 
        echo 'ERROR - pip failed to import dependencies'
        exit 1
    fi

    cd packages
    zip -9mrv packages.zip .
    mv packages.zip ..
    cd ..

    # remove temporary directory and requirements.txt
    rm -rf packages
    
    # add local modules
    echo '... adding all modules from local utils package'
    zip -ru9 packages.zip dependencies -x dependencies/__pycache__/\*

    exit 0
else
    echo 'ERROR - pyenv is not installed'
    exit 1
fi
