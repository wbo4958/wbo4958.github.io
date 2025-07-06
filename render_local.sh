#! /bin/bash

sed -i "s/#theme/theme/" _config.yml
sed -i "s/#path/path/" _config.yml

jekyll serve --incremental

