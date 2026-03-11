@echo off

::
set DAY_FROM_NOW=100
set NUSERS=100
set GROWTH_RATE=10

echo Running eventsim in detached mode...
cd ..\eventsim
docker build -t events .

echo Started streaming events for '%NUSERS%' users...
docker run -it ^
    --network host ^
    events ^
    -c "../eventsim/examples/example-config.json" ^
    -f %DAY_FROM_NOW% ^
    --nusers %NUSERS% ^
    --growth-rate %GROWTH_RATE% ^
    --kafkaBrokerList localhost:9092 ^
    --continuous

echo Done!
pause