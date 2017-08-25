#!/bin/sh
pssh -i -h hosts.txt -l ec2-user -x "-oStrictHostKeyChecking=no  -i /home/yogesh3042/Desktop/PA3.pem" 'java -jar RemoteWorker-0.0.1-SNAPSHOT-jar-with-dependencies.jar --s taskQueue --t 1'
