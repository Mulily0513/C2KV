#!/bin/bash
for item in `cat /proc/1/environ |tr '\0' '\n'`
      do
       export $item
      done