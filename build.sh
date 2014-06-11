#!/bin/bash

mvn clean install -DskipTests=true 

cd storm-dist/binary 

mvn package

cd -
