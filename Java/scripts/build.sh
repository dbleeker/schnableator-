#! /bin/sh

AppName=Recommendations
ProjRootPath=..
ClassPath=${ProjRootPath}/classes:${ProjRootPath}/lib/*

rm -rf ${ProjRootPath}/classes
rm -rf ${ProjRootPath}/bin

mkdir ${ProjRootPath}/classes
mkdir ${ProjRootPath}/bin

javac -classpath ${ClassPath} -deprecation -d ${ProjRootPath}/classes ${ProjRootPath}/src/com/hapyak/recommendations/*.java

jar -cf ${ProjRootPath}/bin/${AppName}.jar -C ${ProjRootPath}/classes .

