# Cómo poner en marcha esta cosa

1. Descargar y descomprimir la base de datos de bakta en /data y configurar el docker-compose para que monte esa carpeta (solo hay que verificar la línea correspondiente).
2. Hacer que docker esté corriendo para poder ejecutar comandos de docker.
3. Ejecutar `docker-compose up --build` para construir y levantar los contenedores en segundo plano. `--build` es necesario para que, si se han hecho cambios en el archivo de python, se hagan efectivos.
4. `test.http` contiene ejemplos de peticiones HTTP que se pueden hacer al servidor. Se puede usar la extensión REST Client de VSCode para ejecutarlas directamente desde el editor.