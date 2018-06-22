#!/bin/sh

(cd java && \
    javac -cp ../inst/java/parso-2.0.9.jar:../inst/java/slf4j-api-1.7.25.jar:. -d . BulkRead.java && \
    jar fvc rparso.jar de && \
    cp -v *.jar ../inst/java)
