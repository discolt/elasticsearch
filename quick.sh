gradle clean jar -Dbuild.snapshot=false -Dlicense.key=LICENSE.txt
cp ./server/build/distributions/elasticsearch-6.8.6.jar ~/elastic/elasticsearch-6.8.6/lib
