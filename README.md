Para construir la imagen:
docker build --tag pythoni .

Para ejecutar:
docker run  --name tweet5 -it -d pythoni /bin/bash

<!-- Para ejecutar:

Opción 1:
docker run  --name tweet5 -it -d -v C:\desarrollo\python\tweetSplit:/app pythoni /bin/bash -->