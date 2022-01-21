# temp-kostyl
docker build -t kostyl .
docker run -e MONGO_DSN="mongodb://USER:PASSWORD@localhost:33233/?authSource=admin&replicaSet=rs0&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false" kostyl --network="host"

