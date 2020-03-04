#!/usr/bin/env bash

mapLogFolder() {
  if [ -n "$1" ]; then
    logFolder=$1
    if [ ! -L "${logFolder}" ]; then
      if [ -e "${logFolder}" ]; then
        echo "This symlink is actually a real folder/file: ${logFolder}"
      else
        ln -s /logs/ "${logFolder}"
      fi
    fi
  fi
  return 0
}
