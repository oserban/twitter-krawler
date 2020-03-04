#!/usr/bin/env bash

scriptPath=$(dirname "$(readlink -f "$0")")
cd "${scriptPath}" || exit

# shellcheck disable=SC1090
source "${scriptPath}/include.sh"
mapLogFolder "${scriptPath}/logs"

if [[ -f "${scriptPath}/.env.sh" ]]; then
    echo "Importing the env properties"
    source "${scriptPath}/.env.sh"
else
    echo "No custom env properties found ... using defaults"
fi;

jar_name="twitter-krawler-*-SNAPSHOT-jar-with-dependencies.jar"

source="${CRAWLER_SOURCE}"
target="${CRAWLER_TARGET}"
opts=" "

jar_path=$( find . -name "${jar_name}" | tail -n 1 )
if [[ -z ${jar_path} && -d target ]]; then
    jar_path=$( find target/ -name "${jar_name}" | tail -n 1 )
fi
if [[ -z ${jar_path} && -d ../target ]]; then
    jar_path=$( find ../target/ -name "${jar_name}" | tail -n 1 )
fi
if [ -z "${jar_path}" ]; then
    echo "Could not find ${jar_name}"
    exit 1
fi

echo "Starting the crawler ..."
echo  "  Source = ${source}"
echo  "  Target = ${target}"

if [ -z "${CRAWLER_CONFIG}" ]; then
    echo "  No crawler config provided"
else
    echo  "  Config = ${CRAWLER_CONFIG}"
    opts="${opts} --config ${CRAWLER_CONFIG} "
fi

java -cp "${jar_path}" org.world.MainKt --source "${source}" --target "${target}" ${opts}