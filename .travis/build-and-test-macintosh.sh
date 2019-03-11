#!/bin/bash

./sbt -J-Xmx2G "project vlm" test || { exit 1; }
