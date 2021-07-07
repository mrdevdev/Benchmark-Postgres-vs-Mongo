# Benchmark-Postgres-vs-Mongo

Nadie duda de lo bueno que es Mongo manejando datos JSON. El uso de índices y su tipo de dato BSON lo coloca muy por encima de muchas otras bases de datos.

Sin embargo, muchas personas se suelen olvidar que hasta hace no más de 5 años, Postgres era considerado superior. Era más rápido en las búsquedas de datos JSON, y ocupaba menos espacio en el almacenamiento. Todo gracias a su tipo de datos JSONB.

En este trabajo práctico, veremos el estado actual en el que ambas bases de datos se encuentran. ¿Sigue siendo Postgres superior? ¿Volvió Mongo a recuperar su trono?

## Requisitos para la ejecución

* Para correr este trabajo práctico, es necesario tener Postgres y MongoDB corriendo en sus puertos default en Localhost.
* A su vez, el TP usará la base de datos "postgres" para hacer las queries en PostgreSQL.
* Hay que tener Python >= 3 instalado.
* Por último, es necesario tener instaladas algunas librerías de Python para el correcto funcionamiento. Con los siguientes comandos se pueden instalar:
```  
pip install Faker
pip install faker_vehicle
pip install pymongo
pip install psycopg2_binary
```

## Ejecución

Una vez cumplidos los requisitos, para correr este trabajo práctico hay que ejecutar el comando:

```
python ./main.py -pg_username <MY_USERNAME> -pg_password <MY_PASS>
```
Siendo <MY_USERNAME> y <MY_PASS> el usuario y contraseña para acceder a su base de datos "postgres".

## ATENCIÓN

La ejecución del programa está puesta para que se inserten 1 millón de registros en las bases de datos. Esta cantidad tarda de 5 a 30 minutos en completarse, dependiendo fuertemente de la capacidad de procesamiento de su computadora.
Si quiere probar con menos (o más) cantidad de registros, modifique el valor de la variable "CANTIDAD_DE_REGISTROS" en el archivo main.py.

## Autores

Este es el proyecto final para la materia Base de Datos 2, desarrollado por:
* Juan Gabriel Griggio
* Ignacio Alberto Méndez
* Franco Navarro
